use async_trait::async_trait;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::AddUserToProjectRequest;

use super::{common_models::DatabaseModel, dataset_object_group::DatasetObject};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[async_trait]
pub trait Database: Send + Sync {
    async fn find_by_key<'de, T: DatabaseModel<'de>>(
        &self,
        key: String,
        value: String,
    ) -> ResultWrapper<Option<Vec<T>>>;
    async fn store<'de, T: DatabaseModel<'de>>(&self, value: T) -> ResultWrapper<T>;
    async fn add_user(&self, request: &AddUserToProjectRequest) -> ResultWrapper<()>;
    async fn find_object(&self, id: String) -> ResultWrapper<DatasetObject>;
    async fn update_field<'de, T: DatabaseModel<'de>>(
        &self,
        find_key: String,
        find_value: String,
        update_field: String,
        update_value: String,
    ) -> ResultWrapper<i64>;
}
