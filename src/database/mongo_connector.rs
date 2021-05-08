use async_trait::async_trait;

use futures::stream::StreamExt;
use mongodb::{
    bson::{from_document, to_document, Bson, Document},
    options::{ClientOptions, FindOptions, UpdateOptions},
    Client,
};
use std::env;

use std::{
    error::Error,
    fmt::{self},
};

use log::error;
use mongodb::{bson::doc, options::FindOneOptions};

use super::{
    common_models::{DatabaseModel, Right, User},
    database::Database,
    dataset_object_group::DatasetObject,
    dataset_object_group::ObjectGroup,
    dataset_object_group::ObjectGroupRevision,
    project_model::ProjectEntry,
};
use crate::SETTINGS;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services;

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct MongoHandler {
    database_name: String,
    mongo_client: mongodb::Client,
}

impl MongoHandler {
    pub async fn new(database_name: String) -> ResultWrapper<Self> {
        let host = SETTINGS
            .read()
            .unwrap()
            .get_str("Database.Mongo.Host")
            .unwrap_or("127.0.0.1".to_string());
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

        let password = env::var("MONGO_PASSWORD").unwrap_or("test123".to_string());

        let mongo_connection_string =
            format!("mongodb://{}:{}@{}:{}", username, password, host, port);
        let client_options = ClientOptions::parse(&mongo_connection_string).await?;
        let client = Client::with_options(client_options)?;

        Ok(MongoHandler {
            database_name: database_name,
            mongo_client: client,
        })
    }

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

    fn collection<'de, T: DatabaseModel<'de>>(&self) -> mongodb::Collection {
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

        let mut csr = match self.collection::<T>().find(query, filter_options).await {
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

        let result = match self.collection::<T>().insert_one(data_document, None).await {
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
        request: &services::AddUserToProjectRequest,
    ) -> Result<(), tonic::Status> {
        let collection = self.collection::<ProjectEntry>();
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

    async fn find_object(&self, id: String) -> Result<DatasetObject, tonic::Status> {
        let filter = doc! {
            "objects.id": id
        };

        let projection = doc! {
            "objects.id": 1,
        };

        let options = FindOneOptions::builder().projection(projection).build();

        let csr = match self
            .collection::<ObjectGroup>()
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

        let object = match csr {
            Some(value) => ObjectGroupRevision::new_from_document(value),
            None => {
                let e = Err(tonic::Status::internal(format!(
                    "error when parsing documents"
                )));
                error!("{:?}", e);
                return e;
            }
        }?;

        return Ok(object.objects[0].clone());
    }

    async fn update_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<i64, tonic::Status> {
        match self.collection::<T>().update_one(query, update, None).await {
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
    ) -> Result<Option<T>, tonic::Status> {
        let filter_options = FindOneOptions::default();

        let csr = match self.collection::<T>().find_one(query, filter_options).await {
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
            None => return Ok(None),
        };

        Ok(Some(entry))
    }

    async fn update_on_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<T, tonic::Status> {
        let option_document = match self
            .collection::<T>()
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
