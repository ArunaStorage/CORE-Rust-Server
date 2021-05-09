use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_objects_service_server::DatasetObjectsService;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{models::Empty, services};
use tonic::Response;

use mongodb::bson::doc;

use crate::{
    auth::authenticator::AuthHandler,
    database::{
        common_models::{Resource, Right},
        database::Database,
        dataset_object_group::ObjectGroup,
    },
};
use crate::{
    database::dataset_object_group::ObjectGroupRevision,
    objectstorage::objectstorage::StorageHandler,
};

use crate::server::util;

pub struct ObjectServer<T: Database + 'static> {
    pub database_client: Arc<T>,
    pub object_handler: Arc<dyn StorageHandler>,
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

        let object_group =
            ObjectGroup::new_from_proto_create(object_group_request, self.database_client.clone())?;
        let inserted_object_group = self.database_client.store(object_group).await?;

        let query = doc! {
            "id": inserted_object_group.id.clone()
        };

        let update = doc! {
            "$inc": {
                "revision_counter": 1
            }
        };

        let updated_object_group = self
            .database_client
            .update_on_field::<ObjectGroup>(query, update)
            .await?;

        let revision = ObjectGroupRevision::new_from_proto_create(
            revision_request,
            &updated_object_group,
            self.object_handler.get_bucket(),
        )?;

        let inserted_revision = self.database_client.store(revision).await?;

        let get_revision_response = services::GetObjectGroupRevisionResponse {
            object_group: Some(updated_object_group.to_proto()?),
            object_group_revision: Some(inserted_revision.to_proto()),
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

        let query = doc! {
            "id": inner_request.object_group_id.clone()
        };

        let update = doc! {
            "$inc": {
                "revision_counter": 1
            }
        };

        let revision_request =
            util::tonic_error_if_not_exists(&inner_request.group_version, "revision")?;

        let updated_object_group = self
            .database_client
            .update_on_field::<ObjectGroup>(query, update)
            .await?;

        let revision = ObjectGroupRevision::new_from_proto_create(
            revision_request,
            &updated_object_group,
            self.object_handler.get_bucket(),
        )?;

        let inserted_revision = self.database_client.store(revision).await?;

        let get_revision_response = services::GetObjectGroupRevisionResponse {
            object_group: Some(updated_object_group.to_proto()?),
            object_group_revision: Some(inserted_revision.to_proto()),
        };

        return Ok(Response::new(get_revision_response));
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

        let query = doc! {
            "id": inner_request.id.clone()
        };

        let object_group_option = &self
            .database_client
            .find_one_by_key::<ObjectGroup>(query)
            .await?;
        let object_group =
            util::tonic_error_if_value_not_found(object_group_option, inner_request.id.as_str())?;

        let revision_query = doc! {
            "revision": object_group.revision_counter,
            "object_group_id": object_group.id.clone()
        };

        let revision_option = &self
            .database_client
            .find_one_by_key::<ObjectGroupRevision>(revision_query)
            .await?;

        let current_revision = match revision_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::invalid_argument(format!(
                    "could not find revision of object group with number {}",
                    object_group.revision_counter
                )))
            }
        };

        let object_group_revision_response = services::GetObjectGroupRevisionResponse {
            object_group: Some(object_group.to_proto()?),
            object_group_revision: Some(current_revision.to_proto()),
        };

        return Ok(Response::new(object_group_revision_response));
    }

    async fn get_current_object_group_revision(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::GetObjectGroupRevisionResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroupRevision,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let query = doc! {
            "id": inner_request.id.clone()
        };

        let object_group_option = &self
            .database_client
            .find_one_by_key::<ObjectGroup>(query)
            .await?;
        let object_group =
            util::tonic_error_if_value_not_found(object_group_option, inner_request.id.as_str())?;

        let revision_query = doc! {
            "id": object_group.revision_counter,
        };

        let revision_option = &self
            .database_client
            .find_one_by_key::<ObjectGroupRevision>(revision_query)
            .await?;

        let current_revision = match revision_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::invalid_argument(format!(
                    "could not find revision of object group with number {}",
                    object_group.revision_counter
                )))
            }
        };

        let object_group_revision_response = services::GetObjectGroupRevisionResponse {
            object_group: Some(object_group.to_proto()?),
            object_group_revision: Some(current_revision.to_proto()),
        };

        return Ok(Response::new(object_group_revision_response));
    }

    async fn get_object_group_revision(
        &self,
        request: tonic::Request<services::GetObjectGroupRevisionRequest>,
    ) -> Result<Response<models::ObjectGroupRevision>, tonic::Status> {
        let inner_request = request.get_ref();
        match inner_request.reference_type() {
            services::ObjectGroupRevisionReferenceType::Revision => {}
            services::ObjectGroupRevisionReferenceType::Version => {
                return Err(tonic::Status::unimplemented("not implemented"))
            }
        }

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroupRevision,
                Right::Read,
                inner_request.revision.clone(),
            )
            .await?;

        let query = doc! {
            "id": inner_request.revision.as_str()
        };

        let object_group_revision = &self
            .database_client
            .find_one_by_key::<ObjectGroupRevision>(query)
            .await?;

        let revision = util::tonic_error_if_value_not_found(
            object_group_revision,
            inner_request.revision.as_str(),
        )?;

        let _group_query = doc! {
            "id": revision.datasete_id.clone()
        };

        //let object_group = util::tonic_error_if_value_not_found(&self.database_client.find_one_by_key::<ObjectGroup>(group_query).await?, revision.datasete_id.as_str())?;

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

        let query = doc! {
            "object_group_id": inner_request.id.as_str()
        };

        let object_group_revisions = self
            .database_client
            .find_by_key::<ObjectGroupRevision>(query)
            .await?;

        let mut proto_revisions = Vec::new();
        for revision in object_group_revisions {
            proto_revisions.push(revision.to_proto())
        }

        let response = services::ObjectGroupRevisions {
            object_group_revision: proto_revisions,
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
}
