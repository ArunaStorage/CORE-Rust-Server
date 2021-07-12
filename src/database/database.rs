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


/// The database trait provides a set of primitves to handle database entries
/// All entries of a model are stored in an individual collection. The functions are mostly generic for all
/// different models. The collection is selected based on the provided or requested type.
#[async_trait]
pub trait Database: Send + Sync {
    /// Reads a set of objects from the database based on the query
    async fn find_by_key<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<Vec<T>, tonic::Status>;
    /// Reads a single object from the database based on the query
    async fn find_one_by_key<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<T, tonic::Status>;
    /// Stores an object in the underlaying database
    async fn store<'de, T: DatabaseModel<'de>>(&self, value: T) -> Result<T, tonic::Status>;
    /// Adds a user to the database
    async fn add_user(&self, request: &AddUserToProjectRequest) -> Result<(), tonic::Status>;
    /// Finds a stored object based on the id from a object revision entry
    async fn find_object(&self, id: &str) -> Result<DatasetObject, tonic::Status>;
    /// Updates a field based on the query and update document
    async fn update_field<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
        update: Document,
    ) -> Result<u64, tonic::Status>;
    // Updates multiple fields
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
    // Updates the status of a database entry
    async fn update_status<'de, T: DatabaseModel<'de>>(
        &self,
        id: &str,
        status: Status,
    ) -> Result<(), tonic::Status>;
    // Deletes a stored database entry
    async fn delete<'de, T: DatabaseModel<'de>>(
        &self,
        query: Document,
    ) -> Result<(), tonic::Status>;
}
