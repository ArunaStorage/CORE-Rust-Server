use super::{authenticator::AuthHandler, oauth2_handler};
use async_trait::async_trait;

enum TokenType {
    OAuth2,
}

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct ProjectAuthzHandler {
    oauth2_handler: oauth2_handler::OAuth2Handler,
}

impl ProjectAuthzHandler {
    pub fn new() -> ResultWrapper<ProjectAuthzHandler> {
        let oauth2 = oauth2_handler::OAuth2Handler::new()?;
        Ok(ProjectAuthzHandler {
            oauth2_handler: oauth2,
        })
    }
}

#[async_trait]
impl AuthHandler for ProjectAuthzHandler {
    async fn authorize(
        metadata: tonic::metadata::MetadataMap,
        resource: crate::database::common_models::Resource,
        right: crate::database::common_models::Right,
        id: String,
    ) -> ResultWrapper<bool> {
        todo!()
    }

    async fn user_id(metadata: tonic::metadata::MetadataMap) -> ResultWrapper<String> {
        todo!()
    }
}
