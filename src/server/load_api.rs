use crate::{auth::authenticator::AuthHandler, database::database_model_wrapper::Database};
use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::object_load_server::ObjectLoad;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::CreateLinkResponse;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use tonic::Response;

use crate::{
    database::{
        data_models::DatasetObjectGroup,
        common_models::{Resource, Right},
    },
};

use crate::objectstorage::objectstorage::StorageHandler;

pub struct LoadServer<T: Database + 'static> {
    pub mongo_client: Arc<T>,
    pub object_handler: Arc<dyn StorageHandler>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<T: Database> ObjectLoad for LoadServer<T> {
    async fn create_upload_link(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::CreateLinkResponse>, tonic::Status> {
        let upload_object = request.get_ref();
        self.auth_handler.authorize(request.metadata(), Resource::Object, Right::Write, upload_object.id.clone()).await?;

        let object_group_option: Option<Vec<DatasetObjectGroup>> = match self
            .mongo_client
            .find_by_key("objects.id".to_string(), upload_object.id.clone())
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
                    upload_object.id.clone()
                )))
            }
        };

        for object_group in object_groups {
            for object in object_group.objects {
                let cloned_object = object.clone();
                if object.id == upload_object.id {
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
            upload_object.id.clone()
        )))
    }

    async fn create_download_link(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::CreateLinkResponse>, tonic::Status> {
        let download_object = request.get_ref();
        self.auth_handler.authorize(request.metadata(), Resource::Object, Right::Read, download_object.id.clone()).await?;

        let object_group_option: Option<Vec<DatasetObjectGroup>> = match self
            .mongo_client
            .find_by_key("objects.id".to_string(), download_object.id.clone())
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
                    download_object.id.clone()
                )))
            }
        };
        for object_group in object_groups {
            for object in object_group.objects {
                let cloned_object = object.clone();
                if object.id == download_object.id {
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
            download_object.id.clone()
        )))
    }
}
