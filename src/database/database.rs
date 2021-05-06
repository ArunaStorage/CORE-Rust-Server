use async_trait::async_trait;
use mongodb::bson::Document;
use serde::{Deserialize, Serialize};

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::AddUserToProjectRequest;

use super::{common_models::DatabaseModel, dataset_object_group::DatasetObject};

#[allow(dead_code)]
pub enum ObjectGroupIDType {
    ObjectGroup,
    ObjectGroupVersion,
}

trait DatabaseSearchValue<'de>: Deserialize<'de> + Serialize + Send + Sync {}

#[async_trait]
pub trait Database: Send + Sync {
    async fn find_by_key<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<Vec<T>, tonic::Status>;
    async fn find_one_by_key<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<Option<T>, tonic::Status>;
    async fn store<'de, T: DatabaseModel<'de>>(&self, value: T) -> Result<T, tonic::Status>;
    async fn add_user(&self, request: &AddUserToProjectRequest) -> Result<(), tonic::Status>;
    async fn find_object(&self, id: String) -> Result<DatasetObject, tonic::Status>;
    async fn update_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<i64, tonic::Status>;
    async fn update_on_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<T, tonic::Status>;
}
