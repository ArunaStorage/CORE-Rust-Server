use bson::doc;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;

use crate::models::dataset_model::DatasetEntry;
use crate::models::dataset_object_group::ObjectGroup;
use crate::models::dataset_version::DatasetVersion;
use crate::{database::database::Database, models::dataset_object_group::ObjectGroupRevision};

use super::common::CommonHandler;

pub type DeleteHandler<T> = CommonHandler<T>;

impl<T> DeleteHandler<T>
where
    T: Database,
{
    pub async fn delete_object_revision(&self, id: String) -> Result<(), tonic::Status> {
        self.update_status::<ObjectGroupRevision>(
            id.as_str(),
            &crate::models::common_models::Status::Deleting,
        )
        .await?;
        let object_revision: ObjectGroupRevision = self.read_entry_by_id(id.as_str()).await?;

        if object_revision.dataset_versions.len() != 0 {
            return Err(tonic::Status::invalid_argument(
                "object group revision could not be deleted, still has associated versions",
            ));
        }

        let mut delete_object_futures = FuturesUnordered::new();
        for object in object_revision.objects {
            delete_object_futures.push(self.object_handler.delete_object(object.location));
        }

        while let Some(value) = delete_object_futures.next().await {
            value?;
        }

        let query = doc! {
            "id": id
        };

        self.database_client
            .delete::<ObjectGroupRevision>(query)
            .await?;

        return Ok(());
    }

    pub async fn delete_object_group(&self, id: String) -> Result<(), tonic::Status> {
        self.update_status::<ObjectGroup>(
            id.as_str(),
            &crate::models::common_models::Status::Deleting,
        )
        .await?;
        let revisions: Vec<ObjectGroupRevision> = self.read_from_parent_entry(id.as_str()).await?;

        let mut delete_object_futures = FuturesUnordered::new();
        for revision in revisions {
            delete_object_futures.push(self.delete_object_revision(revision.id));
        }

        while let Some(value) = delete_object_futures.next().await {
            value?;
        }

        let query = doc! {
            "id": id
        };

        self.database_client.delete::<ObjectGroup>(query).await?;

        return Ok(());
    }

    pub async fn delete_dataset_version(&self, id: String) -> Result<(), tonic::Status> {
        self.database_client
            .update_status::<DatasetVersion>(
                id.as_str(),
                crate::models::common_models::Status::Deleting,
            )
            .await?;

        let query = doc! {};
        let update = doc! {
            "$pull": {
                "dataset_versions": id.as_str()
            }
        };

        self.database_client
            .update_fields::<ObjectGroupRevision>(query, update)
            .await?;

        let query = doc! {
            "id": id.as_str(),
        };

        self.database_client.delete::<DatasetVersion>(query).await?;
        return Ok(());
    }

    pub async fn delete_dataset(&self, id: String) -> Result<(), tonic::Status> {
        self.database_client
            .update_status::<DatasetEntry>(
                id.as_str(),
                crate::models::common_models::Status::Deleting,
            )
            .await?;
        let dataset_versions = self
            .read_from_parent_entry::<DatasetVersion>(id.as_str())
            .await?;
        let mut delete_version_futures = FuturesUnordered::new();
        for version in dataset_versions {
            let delete_req = self.delete_dataset_version(version.id.clone());
            delete_version_futures.push(delete_req);
        }

        while let Some(value) = delete_version_futures.next().await {
            value?;
        }

        let object_groups = self
            .read_from_parent_entry::<ObjectGroup>(id.as_str())
            .await?;
        let mut delete_object_group_futures = FuturesUnordered::new();
        for object_group in object_groups {
            delete_object_group_futures.push(self.delete_object_group(object_group.id))
        }

        while let Some(value) = delete_object_group_futures.next().await {
            value?;
        }

        let query = doc! {
            "id": id
        };

        self.database_client.delete::<DatasetEntry>(query).await?;
        return Ok(());
    }
}
