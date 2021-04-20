use async_trait::async_trait;
use tonic::metadata::MetadataMap;

use crate::database::common_models::{Resource, Right};

/// Authorizes access to individual resources
#[async_trait]
pub trait AuthHandler: Send + Sync {
    /// Authorize access to a specific resource, the authentication information will be read from the tonic metadata
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        resource: Resource,
        right: Right,
        id: String,
    ) -> std::result::Result<(), tonic::Status>;
    /// Returns the user_id of the user based on the authentication information in the metadata map
    async fn user_id(&self, metadata: &MetadataMap) -> std::result::Result<String, tonic::Status>;
}
