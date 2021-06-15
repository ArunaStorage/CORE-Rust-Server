use std::sync::Arc;

use futures::future::try_join_all;
use futures::join;
use log::error;
use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_objects_service_server::DatasetObjectsService;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{models::Empty, services};
use tonic::Request;
use tonic::Response;

use mongodb::bson::doc;

use crate::database::database::Database;
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
    models::dataset_object_group::ObjectGroupRevision,
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

        let query = doc! {
            "id": inner_request.id.as_str()
        };

        let value = match mongodb::bson::to_document(&Status::Deleting) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when converting request to document"
                )));
            }
        };

        let update = doc! {
            "status":  value
        };

        self.database_client
            .update_field::<ObjectGroup>(query.clone(), update)
            .await?;

        let revisions = self
            .database_client
            .find_by_key::<ObjectGroupRevision>(query.clone())
            .await?;

        let mut revision_delete_futures = Vec::new();
        let mut future_count = 1;
        for revision in revisions {
            revision_delete_futures.push(
                self.delete_object_group_revision(Request::new(models::Id { id: revision.id })),
            );
            future_count = future_count + 1;
            if future_count > 100 {
                try_join_all(revision_delete_futures).await?;
                revision_delete_futures = Vec::new();
            }
        }

        if revision_delete_futures.len() > 0 {
            try_join_all(revision_delete_futures).await?;
        }

        self.database_client
            .delete::<ObjectGroup>(query.clone())
            .await?;

        return Ok(Response::new(Empty {}));
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

        let version_check_query = doc! {
            "object_group_ids": inner_request.id.clone()
        };

        match self
            .database_client
            .find_one_by_key::<DatasetVersion>(version_check_query)
            .await?
        {
            Some(_) => {
                return Err(tonic::Status::invalid_argument(
                    "can't delete revision because its still references by a DatasetVersion",
                ))
            }
            None => (),
        };

        let query = doc! {
            "id": inner_request.id.as_str()
        };

        let value = match mongodb::bson::to_document(&Status::Deleting) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when converting request to document"
                )));
            }
        };

        let update = doc! {
            "status":  value
        };

        self.database_client
            .update_field::<ObjectGroupRevision>(query.clone(), update)
            .await?;

        let revision_vec = self
            .database_client
            .find_by_key::<ObjectGroupRevision>(query.clone())
            .await?;

        if revision_vec.len() != 1 {
            return Err(tonic::Status::internal(
                "Unexpected number of dataset revisions found",
            ));
        }

        let revision = &revision_vec[0];
        let mut delete_futures = Vec::new();
        for object in &revision.objects {
            delete_futures.push(self.object_handler.delete_object(object.location.clone()));
        }

        try_join_all(delete_futures).await?;

        self.database_client
            .delete::<ObjectGroupRevision>(query)
            .await?;

        return Ok(Response::new(Empty {}));
    }
}
