use async_trait::async_trait;
use tonic::metadata::MetadataMap;

use crate::database::common_models::{Resource, Right};

#[async_trait]
pub trait AuthHandler: Send + Sync {
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        resource: Resource,
        right: Right,
        id: String,
    ) -> std::result::Result<(), tonic::Status>;
    async fn user_id(&self, metadata: &MetadataMap) -> std::result::Result<String, tonic::Status>;
}
