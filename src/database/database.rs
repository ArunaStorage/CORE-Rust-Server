use async_trait::async_trait;
use mongodb::bson::Document;

use serde::{Deserialize, Serialize};

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::AddUserToProjectRequest;

use crate::models::{
    common_models::{DatabaseModel, Status},
    dataset_object_group::DatasetObject,
};

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
    ) -> Result<T, tonic::Status>;
    async fn store<'de, T: DatabaseModel<'de>>(&self, value: T) -> Result<T, tonic::Status>;
    async fn add_user(&self, request: &AddUserToProjectRequest) -> Result<(), tonic::Status>;
    async fn find_object(&self, id: &str) -> Result<DatasetObject, tonic::Status>;
    async fn update_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<u64, tonic::Status>;
    async fn update_fields<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<u64, tonic::Status>;
    async fn update_on_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<T, tonic::Status>;
    async fn update_status<'de, T: DatabaseModel<'de>>(
        &self,
        id: &str,
        status: Status,
    ) -> Result<(), tonic::Status>;
    async fn delete<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<(), tonic::Status>;
}
