use crate::{auth::authenticator::AuthHandler, database::database_model_wrapper::Database};
use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::{models::{Empty, Object}, services::object_load_server::ObjectLoad};
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::CreateLinkResponse;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use tonic::Response;

use crate::database::{
    common_models::{Resource, Right},
    data_models::DatasetObjectGroup,
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
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Object,
                Right::Write,
                upload_object.id.clone(),
            )
            .await?;

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
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Object,
                Right::Read,
                download_object.id.clone(),
            )
            .await?;

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

    async fn start_multipart_upload(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<Object>, tonic::Status> {
        let download_object = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Object,
                Right::Read,
                download_object.id.clone(),
            )
            .await?;

        let object = match self
            .mongo_client
            .find_object(download_object.id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("could not load dataset object"));
            }
        };

        let upload_id = self.object_handler.init_multipart_upload(object.clone()).await?;
        let updated_fields_count = match self
            .mongo_client
            .update_field::<DatasetObjectGroup>(
                "objects.id".to_string(),
                object.id.clone(),
                "objects.$".to_string(),
                upload_id,
            )
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("could not init dataset load"));
            }
        };

        if updated_fields_count != 1 {
                log::error!("wrong number of updated fields found on upload_id update after multipart upload init");
                return Err(tonic::Status::internal("could not init dataset load"));
        }

        let object = match self.mongo_client.find_object(request.into_inner().id).await{
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("could not init dataset load"));
            }
        };

        return Ok(Response::new(object.to_proto_object()))
    }

    async fn get_multipart_upload_link(
        &self,
        request: tonic::Request<services::GetMultipartUploadRequest>,
    ) -> Result<Response<CreateLinkResponse>, tonic::Status> {
        let upload_request = request.get_ref();
        self.auth_handler
        .authorize(
            request.metadata(),
            Resource::Object,
            Right::Read,
            upload_request.object_id.clone(),
        )
        .await?;

        let object = match self.mongo_client.find_object(upload_request.object_id.clone()).await{
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("could not generate load link"));
            }
        };

        let upload_url = match self.object_handler.upload_multipart_part_link(object.location.clone(), object.upload_id.clone(), upload_request.upload_part).await{
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("could not generate load link"));
            }
        };

        let upload_link_response = CreateLinkResponse{
            object: Some(object.to_proto_object()),
            upload_link: upload_url,
        };

        return Ok(Response::new(upload_link_response))
    }

    async fn complete_multipart_upload(
        &self,
        request: tonic::Request<services::CompleteMultipartRequest>,
    ) -> Result<Response<models::Empty>, tonic::Status> {
        let upload_request = request.get_ref();
        self.auth_handler
        .authorize(
            request.metadata(),
            Resource::Object,
            Right::Read,
            upload_request.object_id.clone(),
        )
        .await?;

        let object = match self.mongo_client.find_object(upload_request.object_id.clone()).await{
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("could not generate load link"));
            }
        };

        self.object_handler.finish_multipart_upload(&object.location, &upload_request.parts, object.upload_id).await?;

        return Ok(Response::new(Empty{}));
    }
}
