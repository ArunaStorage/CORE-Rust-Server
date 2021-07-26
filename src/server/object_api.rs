use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::dataset_objects_service_server::DatasetObjectsService;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{services};
use tonic::Response;

use crate::database::database::Database;
use crate::handler::common::HandlerWrapper;
use crate::models::dataset_object_group::ObjectGroupRevision;
use crate::{
    auth::authenticator::AuthHandler,
    models::{
        common_models::{Resource, Right},
        dataset_object_group::ObjectGroup,
    },
};

use crate::server::util;

pub struct ObjectServer<T: Database + 'static> {
    pub handler_wrapper: Arc<HandlerWrapper<T>>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<'a, T: Database + 'static> DatasetObjectsService for ObjectServer<T> {
    async fn create_object_group(
        &self,
        request: tonic::Request<services::v1::CreateObjectGroupRequest>,
    ) -> Result<Response<services::v1::CreateObjectGroupResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Write,
                inner_request.dataset_id.clone(),
            )
            .await?;

        let create_object_group_req = &services::v1::CreateObjectGroupRequest {
            dataset_id: inner_request.dataset_id.clone(),
            name: inner_request.name.clone(),
            labels: inner_request.labels.clone(),
            metadata: inner_request.metadata.clone(),
            object_group_revision: inner_request.object_group_revision.clone(),
        };

        let object_group = self
            .handler_wrapper
            .create_handler
            .create_object_group(create_object_group_req)
            .await?;

        let revision_id = match &inner_request.object_group_revision {
            Some(request) => self.handler_wrapper.create_handler.create_revision_for_group(request, object_group.id.as_str()).await?.id,
            None => "".to_string(),
        };

        let get_revision_response = services::v1::CreateObjectGroupResponse {
            object_group_id: object_group.id,
            revision_id: revision_id,
        };

        return Ok(Response::new(get_revision_response));
    }

    async fn add_revision_to_object_group(
        &self,
        request: tonic::Request<services::v1::AddRevisionToObjectGroupRequest>,
    ) -> Result<Response<services::v1::AddRevisionToObjectGroupResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroup,
                Right::Write,
                inner_request.object_group_id.clone(),
            )
            .await?;

        let revision_request =
            util::tonic_error_if_not_exists(&inner_request.group_revison, "group_version")?;

        let revision = self
            .handler_wrapper
            .create_handler
            .create_revision_for_group(revision_request, inner_request.object_group_id.as_str())
            .await?;

        let revision_response = services::v1::AddRevisionToObjectGroupResponse {
            revision_id: revision.id,
            revision_number: revision.revision as u64
        };

        return Ok(Response::new(revision_response));
    }

    async fn get_object_group(
        &self,
        request: tonic::Request<services::v1::GetObjectGroupRequest>,
    ) -> Result<Response<services::v1::GetObjectGroupResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroup,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let object_group = self
            .handler_wrapper
            .read_handler
            .read_entry_by_id::<ObjectGroup>(inner_request.id.as_str())
            .await?;

        let object_group_revision_response = services::v1::GetObjectGroupResponse {
            object_group: Some(object_group.to_proto()),
        };

        return Ok(Response::new(object_group_revision_response));
    }

    async fn get_current_object_group_revision(
        &self,
        _request: tonic::Request<services::v1::GetCurrentObjectGroupRevisionRequest>,
    ) -> Result<Response<services::v1::GetCurrentObjectGroupRevisionResponse>, tonic::Status> {
        unimplemented!();
    }

    async fn get_object_group_revision(
        &self,
        request: tonic::Request<services::v1::GetObjectGroupRevisionRequest>,
    ) -> Result<Response<services::v1::GetObjectGroupRevisionResponse>, tonic::Status> {
        let inner_request = request.get_ref();

        let revision_result = match inner_request.reference_type() {
            services::v1::ObjectGroupRevisionReferenceType::Version => Err(
                tonic::Status::unimplemented("version revision type currently not implemented"),
            ),
            services::v1::ObjectGroupRevisionReferenceType::Revision => {
                self.handler_wrapper
                    .read_handler
                    .read_revision(inner_request.revision)
                    .await
            }
            services::v1::ObjectGroupRevisionReferenceType::Id => {
                self.handler_wrapper
                    .read_handler
                    .read_entry_by_id(inner_request.id.as_str())
                    .await
            }
        };

        let revision = revision_result?;

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroupRevision,
                Right::Read,
                revision.id.clone(),
            )
            .await?;

        let response = services::v1::GetObjectGroupRevisionResponse {
            object_group_revision: Some(revision.to_proto()),
            ..Default::default()
        };

        return Ok(Response::new(response));
    }

    async fn get_object_group_revisions(
        &self,
        request: tonic::Request<services::v1::GetObjectGroupRevisionsRequest>,
    ) -> Result<Response<services::v1::GetObjectGroupRevisionsResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroup,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let proto_revision = self
            .handler_wrapper
            .read_handler
            .read_from_parent_entry::<ObjectGroupRevision>(inner_request.id.as_str())
            .await?
            .into_iter()
            .map(|x| x.to_proto())
            .collect();

        let response = services::v1::GetObjectGroupRevisionsResponse {
            object_group_revision: proto_revision,
        };

        return Ok(Response::new(response));
    }

    async fn finish_object_upload(
        &self,
        request: tonic::Request<services::v1::FinishObjectUploadRequest>,
    ) -> Result<Response<services::v1::FinishObjectUploadResponse>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn delete_object_group(
        &self,
        request: tonic::Request<services::v1::DeleteObjectGroupRequest>,
    ) -> Result<Response<services::v1::DeleteObjectGroupResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroup,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        self.handler_wrapper
            .delete_handler
            .delete_object_group(inner_request.id.clone())
            .await?;

        return Ok(Response::new(services::v1::DeleteObjectGroupResponse {}));
    }

    async fn delete_object_group_revision(
        &self,
        request: tonic::Request<services::v1::DeleteObjectGroupRevisionRequest>,
    ) -> Result<Response<services::v1::DeleteObjectGroupRevisionResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroupRevision,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        self.handler_wrapper
            .delete_handler
            .delete_object_revision(inner_request.id.clone())
            .await?;

        return Ok(Response::new(
            services::v1::DeleteObjectGroupRevisionResponse {},
        ));
    }
}
