use async_trait::async_trait;

use crate::database::common_models::{IndexLocation, Location};

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
    fn get_bucket(&self) -> String;
}
