use crate::handler::common::HandlerWrapper;
use crate::{auth::authenticator::AuthHandler, database::database::Database};
use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::object_load_service_server::ObjectLoadService;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use tonic::Response;

use crate::models::common_models::{Resource, Right};

pub struct LoadServer<T: Database + 'static> {
    pub wrapper: Arc<HandlerWrapper<T>>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<T: Database> ObjectLoadService for LoadServer<T> {
    async fn create_upload_link(
        &self,
        request: tonic::Request<services::v1::CreateUploadLinkRequest>,
    ) -> Result<Response<services::v1::CreateUploadLinkResponse>, tonic::Status> {
        let upload_object = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Object,
                Right::Write,
                upload_object.id.clone(),
            )
            .await?;

        let link = self
            .wrapper
            .load_handler
            .create_upload_link(upload_object.id.as_str())
            .await?;
        let object = self
            .wrapper
            .read_handler
            .find_object(upload_object.id.as_str())
            .await?;

        Ok(tonic::Response::new(
            services::v1::CreateUploadLinkResponse {
                upload_link: link,
            },
        ))
    }

    async fn create_download_link(
        &self,
        request: tonic::Request<services::v1::CreateDownloadLinkRequest>,
    ) -> Result<Response<services::v1::CreateDownloadLinkResponse>, tonic::Status> {
        let download_object = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Object,
                Right::Read,
                download_object.id.clone(),
            )
            .await?;

        let link = self
            .wrapper
            .load_handler
            .create_download_link(download_object.id.as_str())
            .await?;
        let object = self
            .wrapper
            .read_handler
            .find_object(download_object.id.as_str())
            .await?;

        Ok(tonic::Response::new(
            services::v1::CreateDownloadLinkResponse {
                upload_link: link,
                object: Some(object.to_proto_object()),
            },
        ))
    }

    async fn start_multipart_upload(
        &self,
        request: tonic::Request<services::v1::StartMultipartUploadRequest>,
    ) -> Result<Response<services::v1::StartMultipartUploadResponse>, tonic::Status> {
        let download_object = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Object,
                Right::Write,
                download_object.id.clone(),
            )
            .await?;

        let object = self
            .wrapper
            .load_handler
            .init_multipart_upload(download_object.id.as_str())
            .await?;

        let response = services::v1::StartMultipartUploadResponse {
            object: Some(object.to_proto_object()),
        };

        return Ok(Response::new(response));
    }

    async fn get_multipart_upload_link(
        &self,
        request: tonic::Request<services::v1::GetMultipartUploadLinkRequest>,
    ) -> Result<Response<services::v1::GetMultipartUploadLinkResponse>, tonic::Status> {
        let upload_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Object,
                Right::Read,
                upload_request.object_id.clone(),
            )
            .await?;

        let object = self
            .wrapper
            .read_handler
            .find_object(upload_request.object_id.as_str())
            .await?;
        let part_link = self
            .wrapper
            .load_handler
            .create_multipart_upload_link(
                upload_request.object_id.as_str(),
                upload_request.upload_part,
            )
            .await?;
        return Ok(Response::new(
            services::v1::GetMultipartUploadLinkResponse {
                object: Some(object.to_proto_object()),
                upload_link: part_link,
            },
        ));
    }

    async fn complete_multipart_upload(
        &self,
        request: tonic::Request<services::v1::CompleteMultipartUploadRequest>,
    ) -> Result<Response<services::v1::CompleteMultipartUploadResponse>, tonic::Status> {
        let upload_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Object,
                Right::Read,
                upload_request.object_id.clone(),
            )
            .await?;

        self.wrapper
            .load_handler
            .finish_multipart_upload(upload_request.object_id.as_str(), &upload_request.parts)
            .await?;

        return Ok(Response::new(
            services::v1::CompleteMultipartUploadResponse {},
        ));
    }
}
