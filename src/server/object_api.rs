use std::sync::Arc;

use futures::future::try_join_all;
use log::error;
use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_objects_service_server::DatasetObjectsService;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{models::Empty, services};
use tonic::Request;
use tonic::Response;

use mongodb::bson::doc;

use crate::database::database::Database;
use crate::handler::common::HandlerWrapper;
use crate::models::common_models::Status;
use crate::{
    auth::authenticator::AuthHandler,
    models::{
        common_models::{Resource, Right},
        dataset_object_group::ObjectGroup,
        dataset_version::DatasetVersion,
    },
};
use crate::{
    models::dataset_object_group::ObjectGroupRevision, objectstorage::objectstorage::StorageHandler,
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
        request: tonic::Request<services::CreateObjectGroupWithRevisionRequest>,
    ) -> Result<Response<services::GetObjectGroupRevisionResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        let object_group_request =
            util::tonic_error_if_not_exists(&inner_request.object_group, "object_group")?;
        let revision_request = util::tonic_error_if_not_exists(
            &inner_request.object_group_version,
            "object_group_revision",
        )?;

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Write,
                object_group_request.dataset_id.clone(),
            )
            .await?;

        let object_group = self
            .handler_wrapper
            .create_handler
            .create_object_group(object_group_request)
            .await?;
        let revision = self
            .handler_wrapper
            .create_handler
            .create_revision_for_group(revision_request, object_group.id.as_str())
            .await?;

        let get_revision_response = services::GetObjectGroupRevisionResponse {
            object_group: Some(object_group.to_proto()),
            object_group_revision: Some(revision.to_proto()),
        };

        return Ok(Response::new(get_revision_response));
    }

    async fn add_revision_to_object_group(
        &self,
        request: tonic::Request<services::AddRevisionToObjectGroupRequest>,
    ) -> Result<Response<services::GetObjectGroupRevisionResponse>, tonic::Status> {
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
            util::tonic_error_if_not_exists(&inner_request.group_version, "group_version")?;

        let object_group = self
            .handler_wrapper
            .read_handler
            .read_entry_by_id::<ObjectGroup>(inner_request.object_group_id.as_str())
            .await?;
        let revision = self
            .handler_wrapper
            .create_handler
            .create_revision_for_group(revision_request, inner_request.object_group_id.as_str())
            .await?;

        let revision_response = services::GetObjectGroupRevisionResponse {
            object_group: Some(object_group.to_proto()),
            object_group_revision: Some(revision.to_proto()),
        };

        return Ok(Response::new(revision_response));
    }

    async fn get_object_group(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::GetObjectGroupRevisionResponse>, tonic::Status> {
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

        let revision = self
            .handler_wrapper
            .read_handler
            .read_revision(object_group.revision_counter)
            .await?;

        let object_group_revision_response = services::GetObjectGroupRevisionResponse {
            object_group: Some(object_group.to_proto()),
            object_group_revision: Some(revision.to_proto()),
        };

        return Ok(Response::new(object_group_revision_response));
    }

    async fn get_current_object_group_revision(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::GetObjectGroupRevisionResponse>, tonic::Status> {
        unimplemented!();
    }

    async fn get_object_group_revision(
        &self,
        request: tonic::Request<services::GetObjectGroupRevisionRequest>,
    ) -> Result<Response<models::ObjectGroupRevision>, tonic::Status> {
        let inner_request = request.get_ref();

        let revision_result = match inner_request.reference_type() {
            services::ObjectGroupRevisionReferenceType::Version => Err(
                tonic::Status::unimplemented("version revision type currently not implemented"),
            ),
            services::ObjectGroupRevisionReferenceType::Revision => {
                self.handler_wrapper
                    .read_handler
                    .read_revision(inner_request.revision)
                    .await
            }
            services::ObjectGroupRevisionReferenceType::Id => {
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

        return Ok(Response::new(revision.to_proto()));
    }

    async fn get_object_group_revisions(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::ObjectGroupRevisions>, tonic::Status> {
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

        let response = services::ObjectGroupRevisions {
            object_group_revision: proto_revision,
        };

        return Ok(Response::new(response));
    }

    async fn finish_object_upload(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<Empty>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn delete_object_group(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<Empty>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroup,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        unimplemented!()
    }

    async fn delete_object_group_revision(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<Empty>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroupRevision,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;
        unimplemented!()
    }
}
