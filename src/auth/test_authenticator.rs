use async_trait::async_trait;

use tonic::metadata::MetadataMap;

use crate::database::{apitoken::APIToken, common_models::{Resource, Right}};

use super::authenticator::AuthHandler;

pub struct TestAuthenticator {}

#[async_trait]
impl AuthHandler for TestAuthenticator {
    async fn authorize(
        &self,
        _metadata: &MetadataMap,
        _resource: Resource,
        _right: Right,
        _id: String,
    ) -> std::result::Result<(), tonic::Status> {
        Ok(())
    }

    async fn user_id(&self, _metadata: &MetadataMap) -> std::result::Result<String, tonic::Status> {
        Ok("testuser".to_string())
    }

    async fn project_id_from_api_token(&self, metadata: &MetadataMap) -> std::result::Result<crate::database::apitoken::APIToken, tonic::Status> {
        Ok(APIToken{
            ..Default::default()
        })
    }
}
