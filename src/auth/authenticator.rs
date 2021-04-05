use async_trait::async_trait;
use tonic::metadata::MetadataMap;

use crate::database::common_models::{Resource, Right};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[async_trait]
pub trait AuthHandler {
    async fn authorize(
        metadata: MetadataMap,
        resource: Resource,
        right: Right,
        id: String,
    ) -> ResultWrapper<bool>;
    async fn user_id(metadata: MetadataMap) -> ResultWrapper<String>;
}
