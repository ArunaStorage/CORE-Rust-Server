use crate::database::database_model_wrapper::Database;
use std::sync::Arc;

use mongodb::options::ResolverConfig;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::object_load_server::ObjectLoad;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::CreateLinkResponse;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use tonic::Response;

use crate::database::common_models::DatabaseHandler;
use crate::{
    database::{
        data_models::DatasetEntry, data_models::DatasetObjectGroup, data_models::ProjectEntry,
        mongo_connector::MongoHandler,
    },
    objectstorage::objectstorage,
};

use crate::objectstorage::objectstorage::StorageHandler;

pub struct LoadServer {
    pub mongo_client: Arc<MongoHandler>,
    pub object_handler: Arc<dyn StorageHandler>,
}

#[tonic::async_trait]
impl ObjectLoad for LoadServer {
    async fn create_upload_link(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::CreateLinkResponse>, tonic::Status> {
        let id = request.into_inner().id;

        let object_group_option: Option<Vec<DatasetObjectGroup>> = match self
            .mongo_client
            .find_by_key("objects.id".to_string(), id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let object_groups = match object_group_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::not_found(format!(
                    "Could not find {}",
                    id.clone()
                )))
            }
        };

        for object_group in object_groups {
            for object in object_group.objects {
                let cloned_object = object.clone();
                if object.id == id {
                    let link = match self
                        .object_handler
                        .create_upload_link(object.location)
                        .await
                    {
                        Ok(link) => link,
                        Err(e) => {
                            log::error!("{:?}", e);
                            return Err(tonic::Status::internal(format!("{:?}", e)));
                        }
                    };

                    let message = CreateLinkResponse {
                        object: Some(cloned_object.to_proto_object()),
                        upload_link: link,
                    };

                    return Ok(Response::new(message));
                }
            }
        }

        Err(tonic::Status::not_found(format!(
            "Could not find {}",
            id.clone()
        )))
    }

    async fn create_download_link(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::CreateLinkResponse>, tonic::Status> {
        let id = request.into_inner().id;

        let object_group_option: Option<Vec<DatasetObjectGroup>> = match self
            .mongo_client
            .find_by_key("objects.id".to_string(), id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let object_groups = match object_group_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::not_found(format!(
                    "Could not find {}",
                    id.clone()
                )))
            }
        };
        for object_group in object_groups {
            for object in object_group.objects {
                let cloned_object = object.clone();
                if object.id == id {
                    let link = match self
                        .object_handler
                        .create_download_link(object.location)
                        .await
                    {
                        Ok(link) => link,
                        Err(e) => {
                            log::error!("{:?}", e);
                            return Err(tonic::Status::internal(format!("{:?}", e)));
                        }
                    };

                    let message = CreateLinkResponse {
                        object: Some(cloned_object.to_proto_object()),
                        upload_link: link,
                    };

                    return Ok(Response::new(message));
                }
            }
        }

        Err(tonic::Status::not_found(format!(
            "Could not find {}",
            id.clone()
        )))
    }
}
