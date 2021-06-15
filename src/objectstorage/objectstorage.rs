use async_trait::async_trait;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::CompletedParts;

use crate::models::{
    common_models::{IndexLocation, Location},
    dataset_object_group::DatasetObject,
};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[async_trait]
pub trait StorageHandler: Send + Sync {
    async fn create_location(
        &self,
        project_id: String,
        dataset_id: String,
        object_id: String,
        filename: String,
        index: Option<IndexLocation>,
    ) -> ResultWrapper<Location>;
    async fn create_download_link(&self, location: Location) -> ResultWrapper<String>;
    async fn create_upload_link(&self, location: Location) -> ResultWrapper<String>;
    async fn init_multipart_upload(
        &self,
        location: DatasetObject,
    ) -> std::result::Result<String, tonic::Status>;
    async fn upload_multipart_part_link(
        &self,
        location: Location,
        upload_id: String,
        upload_part: i64,
    ) -> std::result::Result<String, tonic::Status>;
    async fn finish_multipart_upload(
        &self,
        location: &Location,
        objects: &Vec<CompletedParts>,
        upload_id: String,
    ) -> Result<(), tonic::Status>;
    async fn delete_object(&self, location: Location) -> std::result::Result<(), tonic::Status>;
    fn get_bucket(&self) -> String;
}
