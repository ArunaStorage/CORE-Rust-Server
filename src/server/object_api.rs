use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_objects_service_server::DatasetObjectsService;
use tonic::Response;

use crate::database::{database_model_wrapper::Database, mongo_connector::MongoHandler};
use crate::{
    database::common_models::DatabaseHandler, database::data_models::DatasetObjectGroup,
    objectstorage::objectstorage::StorageHandler,
};

pub struct ObjectServer {
    pub mongo_client: Arc<MongoHandler>,
    pub object_handler: Arc<dyn StorageHandler>,
}

#[tonic::async_trait]
impl<'a> DatasetObjectsService for ObjectServer {
    async fn create_object_heritage(
        &self,
        request: tonic::Request<services::CreateObjectHeritageRequest>,
    ) -> Result<Response<models::ObjectHeritage>, tonic::Status> {
        todo!()
    }

    async fn create_object_group(
        &self,
        request: tonic::Request<services::CreateObjectGroupRequest>,
    ) -> Result<Response<models::ObjectGroup>, tonic::Status> {
        let object_group = match DatasetObjectGroup::new_from_proto_create(
            request.into_inner(),
            self.object_handler.get_bucket(),
            self.mongo_client.clone(),
        ) {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let inserted_group = match self.mongo_client.store(object_group).await {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let proto_group = inserted_group.to_proto();

        return Ok(Response::new(proto_group));
    }

    async fn get_object_group(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::ObjectGroup>, tonic::Status> {
        let id = request.into_inner().id;

        let object_group_option: Option<Vec<DatasetObjectGroup>> = match self
            .mongo_client
            .find_by_key("id".to_string(), id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let object_group = match object_group_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::not_found(format!(
                    "Could not find objectgroup with id: {}",
                    id.clone()
                )))
            }
        };

        let object = &object_group[0];

        Ok(Response::new(object.to_proto()))
    }

    async fn finish_object_upload(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Empty>, tonic::Status> {
        todo!()
    }
}
