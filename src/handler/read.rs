use bson::doc;

use crate::{
    database::database::Database,
    models::{
        apitoken::APIToken,
        common_models::DatabaseModel,
        dataset_object_group::{DatasetObject, ObjectGroup, ObjectGroupRevision},
        project_model::ProjectEntry,
    },
};

use super::common::CommonHandler;

pub type ReadHandler<T> = CommonHandler<T>;

impl<T> ReadHandler<T>
where
    T: Database,
{
    pub async fn read_entry_by_id<'de, K: DatabaseModel<'de>>(
        &self,
        id: &str,
    ) -> Result<K, tonic::Status> {
        let query = doc! {
            "id": id
        };

        return self.database_client.find_one_by_key(query).await;
    }

    pub async fn read_entries_by_id<'de, K: DatabaseModel<'de>>(
        &self,
        id: &str,
    ) -> Result<Vec<K>, tonic::Status> {
        let query = doc! {
            "id": id
        };

        return self.database_client.find_by_key(query).await;
    }

    pub async fn read_from_parent_entry<'de, K: DatabaseModel<'de>>(
        &self,
        parent_id: &str,
    ) -> Result<Vec<K>, tonic::Status> {
        let query = doc! {
            K::get_parent_field_name()?: parent_id,
        };

        return self.database_client.find_by_key(query).await;
    }

    pub async fn read_user_projects(
        &self,
        user_id: &str,
    ) -> Result<Vec<ProjectEntry>, tonic::Status> {
        let query = doc! {
            "users.user_id": user_id
        };

        let projects = self
            .database_client
            .find_by_key::<ProjectEntry>(query)
            .await?;

        return Ok(projects);
    }

    pub async fn read_user_api_token(&self, user_id: &str) -> Result<Vec<APIToken>, tonic::Status> {
        let query = doc! {
            "user_id": user_id
        };

        return self.database_client.find_by_key(query).await;
    }

    pub async fn find_object(&self, id: &str) -> Result<DatasetObject, tonic::Status> {
        return self.database_client.find_object(id).await;
    }

    pub async fn read_revision(
        &self,
        object_group_id: &str,
        revision: i64,
    ) -> Result<ObjectGroupRevision, tonic::Status> {
        let query = doc! {
            "object_group_id": object_group_id,
            "revision": revision
        };

        return self.database_client.find_one_by_key(query).await;
    }

    pub async fn read_current_revision(
        &self,
        object_group_id: &str,
    ) -> Result<ObjectGroupRevision, tonic::Status> {
        let object_group = self
            .read_entry_by_id::<ObjectGroup>(object_group_id)
            .await?;
        return self
            .read_revision(object_group_id, object_group.revision_counter - 1)
            .await;
    }
}
