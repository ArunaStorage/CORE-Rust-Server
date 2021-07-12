use crate::handler::common::HandlerWrapper;
use crate::{auth::authenticator::AuthHandler, database::database::Database};
use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::CreateLinkResponse;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models::{Empty, Object},
    services::object_load_server::ObjectLoad,
};

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use tonic::Response;

use crate::models::common_models::{Resource, Right};

pub struct LoadServer<T: Database + 'static> {
    pub wrapper: Arc<HandlerWrapper<T>>,
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

        Ok(tonic::Response::new(services::CreateLinkResponse {
            upload_link: link,
            object: Some(object.to_proto_object()),
        }))
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

        Ok(tonic::Response::new(services::CreateLinkResponse {
            upload_link: link,
            object: Some(object.to_proto_object()),
        }))
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
                Right::Write,
                download_object.id.clone(),
            )
            .await?;

        let object = self
            .wrapper
            .load_handler
            .init_multipart_upload(download_object.id.as_str())
            .await?;

        return Ok(Response::new(object.to_proto_object()));
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
        return Ok(Response::new(CreateLinkResponse {
            object: Some(object.to_proto_object()),
            upload_link: part_link,
        }));
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

        self.wrapper
            .load_handler
            .finish_multipart_upload(upload_request.object_id.as_str(), &upload_request.parts)
            .await?;

        return Ok(Response::new(Empty {}));
    }
}
