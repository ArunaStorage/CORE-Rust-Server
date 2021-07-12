use crate::database::database::Database;
use crate::models::apitoken::APIToken;
use crate::models::common_models::Right;
use crate::models::dataset_model::DatasetEntry;
use crate::models::dataset_object_group::ObjectGroup;
use crate::models::dataset_object_group::ObjectGroupRevision;
use crate::models::dataset_version::DatasetVersion;
use crate::models::project_model::ProjectEntry;
use bson::doc;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::{
    CreateDatasetRequest, CreateObjectGroupRequest, CreateObjectGroupRevisionRequest,
    CreateProjectRequest, ReleaseDatasetVersionRequest,
};

use super::common::CommonHandler;

/// Handles create associated tasks for the individual models
pub type CreateHandler<T> = CommonHandler<T>;

impl<T> CreateHandler<T>
where
    T: Database,
{
    pub async fn create_project(
        &self,
        project: &CreateProjectRequest,
        user_id: String,
    ) -> Result<ProjectEntry, tonic::Status> {
        let project_entry = ProjectEntry::new_from_proto_create(project, user_id)?;
        return self.database_client.store(project_entry).await;
    }

    pub async fn create_dataset(
        &self,
        dataset: &CreateDatasetRequest,
    ) -> Result<DatasetEntry, tonic::Status> {
        let dataset_entry = DatasetEntry::new_from_proto_create(dataset)?;
        return self.database_client.store(dataset_entry).await;
    }

    pub async fn create_object_group(
        &self,
        object_group_request: &CreateObjectGroupRequest,
    ) -> Result<ObjectGroup, tonic::Status> {
        let object_group = ObjectGroup::new_from_proto_create(object_group_request)?;
        return self.database_client.store(object_group).await;
    }

    pub async fn create_revision_for_group(
        &self,
        revision_request: &CreateObjectGroupRevisionRequest,
        parent_object_group_id: &str,
    ) -> Result<ObjectGroupRevision, tonic::Status> {
        let query = doc! {
            "id": parent_object_group_id
        };

        // If a new revision is created it is necessary to update the revision counter as well.
        let update = doc! {
            "$inc": {
                "revision_counter": 1
            }
        };

        let object_group = self
            .database_client
            .update_on_field::<ObjectGroup>(query, update)
            .await?;

        let revision_entry = ObjectGroupRevision::new_from_proto_create(
            revision_request,
            &object_group,
            self.object_handler.get_bucket(),
        )?;
        return self.database_client.store(revision_entry).await;
    }

    pub async fn create_datatset_version(
        &self,
        version_request: &ReleaseDatasetVersionRequest,
    ) -> Result<DatasetVersion, tonic::Status> {
        let dataset_version_entry = DatasetVersion::new_from_proto_create(version_request)?;
        let inserted_dataset_version = self.database_client.store(dataset_version_entry).await?;

        let mut poll_revision_version_add = FuturesUnordered::new();

        for revision_id_chunk in version_request.revision_ids.chunks(1000) {
            let query = doc! {
                "id": {
                    "$in": revision_id_chunk
                }
            };

            let update = doc! {
                "$addToSet": {
                    "dataset_versions": inserted_dataset_version.id.clone()
                }
            };

            let update_request = self
                .database_client
                .update_field::<ObjectGroupRevision>(query, update);

            poll_revision_version_add.push(update_request)
        }

        while let Some(value) = poll_revision_version_add.next().await {
            value?;
        }

        return Ok(inserted_dataset_version);
    }

    pub async fn create_api_token(
        &self,
        user_id: &str,
        rights: Vec<Right>,
        project_id: &str,
    ) -> Result<APIToken, tonic::Status> {
        let api_token = APIToken::new(user_id, rights, project_id)?;
        let inserted_api_token = self.database_client.store::<APIToken>(api_token).await?;

        Ok(inserted_api_token)
    }
}
