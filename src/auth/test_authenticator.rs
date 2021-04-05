use async_trait::async_trait;

use tonic::metadata::MetadataMap;

use crate::database::common_models::{Resource, Right};

use super::authenticator::AuthHandler;

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct TestAuthenticator {}

#[async_trait]
impl AuthHandler for TestAuthenticator {
    async fn authorize(
        _metadata: MetadataMap,
        _resource: Resource,
        _right: Right,
        _id: String,
    ) -> ResultWrapper<bool> {
        Ok(true)
    }

    async fn user_id(_metadata: MetadataMap) -> ResultWrapper<String> {
        Ok("testuser".to_string())
    }
}
