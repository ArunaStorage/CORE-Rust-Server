use async_trait::async_trait;

use std::convert::TryFrom;

use futures::stream::StreamExt;
use mongodb::{
    bson::{from_document, to_document, Bson, Document},
    options::{FindOptions, ServerAddress, UpdateOptions},
    Client,
};
use std::{env, time::Duration};

use std::{
    error::Error,
    fmt::{self},
};

use log::error;
use mongodb::{bson::doc, options::FindOneOptions};

use super::database::Database;

use crate::{
    models::{
        common_models::{DatabaseModel, Right, Status, User},
        dataset_object_group::{DatasetObject, ObjectGroupRevision},
        project_model::ProjectEntry,
    },
    SETTINGS,
};

use scienceobjectsdb_rust_api::sciobjectsdbapi::services;

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct MongoHandler {
    database_name: String,
    mongo_client: mongodb::Client,
}

impl MongoHandler {
    /// Initiates a new MongoDB handler
    /// Behaves like new_with_db_name but the database name is also read from the configuration file
    pub async fn new() -> Result<Self, tonic::Status> {
        let database_name = SETTINGS
            .read()
            .unwrap()
            .get_str("Database.Mongo.Database")
            .unwrap_or("objectsdb".to_string());

        return MongoHandler::new_with_db_name(database_name).await;
    }

    /// Initiates a new MongoDB handler
    /// The name of the mongo database is provided
    /// All other parameters are read from the configuration file
    pub async fn new_with_db_name(database_name: String) -> Result<Self, tonic::Status> {
        let host = SETTINGS
            .read()
            .unwrap()
            .get_str("Database.Mongo.Host")
            .unwrap_or("localhost".to_string());
        let username = SETTINGS
            .read()
            .unwrap()
            .get_str("Database.Mongo.Username")
            .unwrap_or("root".to_string());
        let port = SETTINGS
            .read()
            .unwrap()
            .get_int("Database.Mongo.Port")
            .unwrap_or(27017);
        let source = SETTINGS
            .read()
            .unwrap()
            .get_str("Database.Mongo.Source")
            .unwrap_or("admin".to_string());

        let password = env::var("MONGO_PASSWORD").unwrap_or("test123".to_string());

        let port_u16 = match u16::try_from(port) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                std::process::exit(2);
            }
        };

        let host = ServerAddress::Tcp {
            host: host,
            port: Some(port_u16),
        };

        let client_credentials = mongodb::options::Credential::builder()
            .username(username)
            .password(password)
            .source(source)
            .build();
        let client_options = mongodb::options::ClientOptions::builder()
            .credential(client_credentials)
            .connect_timeout(Duration::from_millis(500))
            .hosts(vec![host])
            .build();

        let client = match Client::with_options(client_options) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                std::process::exit(1)
            }
        };

        Ok(MongoHandler {
            database_name: database_name,
            mongo_client: client,
        })
    }

    /// Returns an entry based on the internal ID of an inserted object
    /// This can be used to get the model of an inserted object since MongoDB will only return the ObjectID of the inserted object
    async fn get_model_entry_internal_id<'de, T: DatabaseModel<'de>>(
        &self,
        id: Bson,
    ) -> ResultWrapper<Option<T>> {
        let query = doc! {"_id": id};
        let mut filter_option = FindOneOptions::default();
        let projection = doc! {"_id": 0};

        filter_option.projection = Some(projection);

        let collection_name = T::get_model_name().unwrap();

        let data = match self
            .mongo_client
            .database(&self.database_name)
            .collection(&collection_name)
            .find_one(query, filter_option)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(Box::new(SimpleError::new(&format!("{:?}", e))));
            }
        };

        let document = match data {
            Some(value) => value,
            None => return Ok(None),
        };

        let model = match T::new_from_document(document) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(Box::new(SimpleError::new(&format!("{:?}", e))));
            }
        };

        return Ok(Some(model));
    }

    ///Transforms the given Document into the associated internal model representation
    #[allow(dead_code)]
    pub fn to_model<'de, T: DatabaseModel<'de>>(
        &self,
        document: Option<Document>,
    ) -> std::result::Result<Option<T>, tonic::Status> {
        let document = match document {
            Some(value) => value,
            None => return Ok(None),
        };

        let model = match T::new_from_document(document) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "error unwrapping message from database",
                ));
            }
        };

        return Ok(Some(model));
    }

    /// Returns the MongoDB collection that handles a specific model type
    fn collection<'de, T, V>(&self) -> mongodb::Collection<V>
    where
        T: DatabaseModel<'de>,
    {
        self.mongo_client
            .database(&self.database_name)
            .collection(&T::get_model_name().unwrap())
    }
}

#[async_trait]
impl Database for MongoHandler {
    async fn find_by_key<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<Vec<T>, tonic::Status> {
        let mut entries = Vec::new();
        let filter_options = FindOptions::default();

        let mut csr = match self
            .collection::<T, Document>()
            .find(query, filter_options)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                error!("{}", e);
                return Err(tonic::Status::internal(format!(
                    "error when searching found documents"
                )));
            }
        };

        while let Some(result) = csr.next().await {
            match result {
                Ok(document) => {
                    let datasetentry = T::new_from_document(document)?;
                    entries.push(datasetentry);
                }
                Err(e) => {
                    error!("{}", e);
                    return Err(tonic::Status::internal(format!(
                        "error when parsing documents"
                    )));
                }
            }
        }

        Ok(entries)
    }

    async fn store<'de, T: DatabaseModel<'de>>(&self, value: T) -> Result<T, tonic::Status> {
        let data_document = match value.to_document() {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when converting request to document"
                )));
            }
        };

        let result = match self
            .collection::<T, Document>()
            .insert_one(data_document, None)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when inserting document"
                )));
            }
        };
        let insert_result = match self.get_model_entry_internal_id(result.inserted_id).await {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "could not extract internal id from inserted document"
                )));
            }
        };

        let inserted_model = match insert_result {
            Some(value) => value,
            None => {
                return Err(tonic::Status::internal(format!(
                    "could not extract document from internal id of inserted document"
                )));
            }
        };

        return Ok(inserted_model);
    }

    async fn add_user(
        &self,
        request: &services::v1::AddUserToProjectRequest,
    ) -> Result<(), tonic::Status> {
        let collection = self.collection::<ProjectEntry, Document>();
        let filter = doc! {
            "id": request.project_id.clone(),
        };

        let user = User {
            user_id: request.user_id.clone(),
            rights: vec![Right::Read, Right::Write],
        };

        let user_document = match to_document(&user) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "could not convert user object to internal representation"
                )));
            }
        };

        let insert = doc! {
            "$addToSet": {"users": user_document}
        };

        let options = UpdateOptions::default();

        match collection.update_one(filter, insert, options).await {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "could not update user object"
                )));
            }
        };

        return Ok(());
    }

    async fn find_object(&self, id: &str) -> Result<DatasetObject, tonic::Status> {
        let filter = doc! {
            "objects.id": id
        };

        let projection = doc! {
            "objects.$": 1,
            "_id": 0,
        };

        let options = FindOneOptions::builder().projection(projection).build();

        let csr = match self
            .collection::<ObjectGroupRevision, Document>()
            .find_one(filter, options)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "could find requested object"
                )));
            }
        };

        let document = csr.ok_or(tonic::Status::internal(
            "could not find requested dataset object",
        ))?;
        let objects_list = match document.get_array("objects") {
            Ok(value) => value.to_owned(),
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "could not read requested dataset object",
                ));
            }
        };

        if objects_list.len() != 1 {
            error!(
                "wrong number of objects found in objects list: found {} objects for object_id {}",
                objects_list.len(),
                id
            );
            return Err(tonic::Status::internal(
                "could not read requested dataset object",
            ));
        }

        let bson_object = &objects_list[0];
        let object: DatasetObject = match bson::from_bson(bson_object.to_owned()) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "could not read requested dataset object",
                ));
            }
        };

        return Ok(object);
    }

    async fn update_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<u64, tonic::Status> {
        match self
            .collection::<T, Document>()
            .update_one(query, update, None)
            .await
        {
            Ok(value) => return Ok(value.modified_count),
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when trying to update document"
                )));
            }
        };
    }

    async fn find_one_by_key<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<T, tonic::Status> {
        let filter_options = FindOneOptions::default();

        let csr = match self
            .collection::<T, Document>()
            .find_one(query, filter_options)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when trying to find entry"
                )));
            }
        };

        let entry = match csr {
            Some(value) => T::new_from_document(value)?,
            None => {
                return Err(tonic::Status::not_found(format!(
                    "could not find requested document. type: {}",
                    T::get_model_name()?
                )))
            }
        };

        Ok(entry)
    }

    async fn update_on_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<T, tonic::Status> {
        let option_document = match self
            .collection::<T, Document>()
            .find_one_and_update(query, update, None)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when trying to update document"
                )));
            }
        };

        let document = match option_document {
            Some(value) => value,
            None => {
                return Err(tonic::Status::internal(format!(
                    "could not find value during update"
                )));
            }
        };

        let option_value: T = match from_document(document) {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when trying to convert document to type after update"
                )));
            }
        };

        return Ok(option_value);
    }

    async fn delete<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<(), tonic::Status> {
        match self
            .collection::<T, Document>()
            .delete_one(query, None)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                log::error!("{:?}", e);
                tonic::Status::internal(format!("could not delete object"));
            }
        }

        return Ok(());
    }

    async fn update_status<'de, T: DatabaseModel<'de>>(
        &self,
        id: &str,
        status: Status,
    ) -> Result<(), tonic::Status> {
        let query = doc! {
            "id": id
        };

        let value = match mongodb::bson::to_bson(&status) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when converting request to document"
                )));
            }
        };

        let update = doc! {
            "$set": {
                "status":  value
            }
        };

        match self
            .collection::<T, Document>()
            .update_one(query, update, None)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!("error on status update")));
            }
        }

        return Ok(());
    }

    async fn update_fields<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<u64, tonic::Status> {
        match self
            .collection::<T, Document>()
            .update_many(query, update, None)
            .await
        {
            Ok(value) => return Ok(value.modified_count),
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when trying to update document"
                )));
            }
        };
    }
}

#[derive(Debug)]
struct SimpleError {
    details: String,
}

impl SimpleError {
    fn new(msg: &str) -> SimpleError {
        SimpleError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for SimpleError {
    fn description(&self) -> &str {
        &self.details
    }
}
