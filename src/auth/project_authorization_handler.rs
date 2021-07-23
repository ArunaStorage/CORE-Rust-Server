use std::{error::Error, fmt, sync::Arc};

use log::error;
use mongodb::bson::doc;
use std::collections::HashSet;
use tonic::metadata::MetadataMap;

use crate::{
    database::database::Database,
    models::{
        apitoken::APIToken,
        common_models::Right,
        dataset_model::DatasetEntry,
        dataset_object_group::{ObjectGroup, ObjectGroupRevision},
        dataset_version::DatasetVersion,
        project_model::ProjectEntry,
    },
};

use super::{authenticator::AuthHandler, oauth2_handler};
use async_trait::async_trait;

///Kind of token that has been found in the metadata
#[allow(dead_code)]
enum TokenType {
    OAuth2,
}

pub const API_TOKEN_ENTRY_KEY: &str = "API_TOKEN";
pub const USER_TOKEN_ENTRY_KEY: &str = "AccessToken";

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Authorizes access to resources based on user right on project level
/// It will resolve each resource to its project. Based on that it will grant access or respond with an
/// grpc error.
pub struct ProjectAuthzHandler<T: Database> {
    oauth2_handler: oauth2_handler::OAuth2Handler,
    database_handler: Arc<T>,
}

impl<T: Database> ProjectAuthzHandler<T> {
    pub fn new(database: Arc<T>) -> ResultWrapper<ProjectAuthzHandler<T>> {
        let oauth2 = oauth2_handler::OAuth2Handler::new()?;
        Ok(ProjectAuthzHandler {
            oauth2_handler: oauth2,
            database_handler: database,
        })
    }

    async fn authorize_from_user_token(
        &self,
        id: String,
        metadata: &MetadataMap,
        right: crate::models::common_models::Right,
    ) -> Result<(), tonic::Status> {
        let query = doc! {
            "id": &id,
        };

        let project: ProjectEntry = match self.database_handler.find_one_by_key(query).await {
            Ok(value) => value,
            Err(_) => {
                return Err(tonic::Status::internal(
                    "could not authorize requested action",
                ));
            }
        };

        let user_id = self.user_id(metadata).await?;

        for user in project.users {
            if user.user_id == user_id {
                for user_right in user.rights {
                    if user_right == right {
                        return Ok(());
                    }
                }
            }
        }

        return Err(tonic::Status::permission_denied(
            "could not authorize requested action",
        ));
    }

    async fn authorize_from_api_token(
        &self,
        metadata: &MetadataMap,
        project_id: String,
        requested_rights: Vec<Right>,
    ) -> Result<(), tonic::Status> {
        let db_token = self.project_id_from_api_token(metadata).await?;
        if db_token.project_id != project_id {
            return Err(tonic::Status::permission_denied(
                "could not authorize for requested project",
            ));
        }

        let mut rights_hash_set = HashSet::new();
        for right in db_token.rights {
            rights_hash_set.insert(right);
        }

        for requested_right in requested_rights {
            if !rights_hash_set.contains(&requested_right) {
                return Err(tonic::Status::permission_denied(
                    "could not authorize for request project",
                ));
            };
        }

        return Ok(());
    }

    async fn project_id_of_dataset(&self, id: String) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id
        };

        let dataset: DatasetEntry = self.database_handler.find_one_by_key(query).await?;

        return Ok(dataset.project_id);
    }

    async fn project_id_of_object_group(&self, id: String) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id,
        };

        let dataset_group: ObjectGroup = self.database_handler.find_one_by_key(query).await?;

        return self.project_id_of_dataset(dataset_group.id.clone()).await;
    }

    async fn project_id_of_object(&self, id: String) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id,
        };

        let dataset_group: ObjectGroupRevision =
            self.database_handler.find_one_by_key(query).await?;

        return self.project_id_of_dataset(dataset_group.id.clone()).await;
    }

    async fn project_id_of_dataset_version(&self, id: String) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id,
        };

        let dataset_version: DatasetVersion = self.database_handler.find_one_by_key(query).await?;

        return self
            .project_id_of_dataset(dataset_version.dataset_id.clone())
            .await;
    }

    async fn project_id_of_object_group_revision(
        &self,
        id: String,
    ) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id,
        };

        let object_groups_version: ObjectGroupRevision =
            self.database_handler.find_one_by_key(query).await?;

        let project_id = self
            .project_id_of_dataset(object_groups_version.datasete_id)
            .await?;
        Ok(project_id)
    }

    async fn user_id_from_access_token(
        &self,
        metadata: &MetadataMap,
    ) -> std::result::Result<String, tonic::Status> {
        let access_token = match metadata.get(USER_TOKEN_ENTRY_KEY) {
            Some(value) => value.to_str().unwrap(),
            None => {
                return Err(tonic::Status::internal(
                    "could not get user_id_from access token",
                ));
            }
        };

        let user_id = match self
            .oauth2_handler
            .parse_user_id_from_token(access_token.to_string())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "could not get user_id_from access token",
                ));
            }
        };
        return Ok(user_id);
    }

    async fn user_id_from_api_token(
        &self,
        metadata: &MetadataMap,
    ) -> std::result::Result<String, tonic::Status> {
        let api_token = match metadata.get(API_TOKEN_ENTRY_KEY) {
            Some(value) => value.to_str().unwrap(),
            None => {
                return Err(tonic::Status::internal("could not read API token"));
            }
        };

        let query = doc! {
            "token": api_token
        };

        let db_token = match self
            .database_handler
            .find_one_by_key::<APIToken>(query)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::unauthenticated(
                    "could not authenticate from api_token",
                ));
            }
        };

        return Ok(db_token.user_id);
    }
}

#[async_trait]
impl<T: Database> AuthHandler for ProjectAuthzHandler<T> {
    async fn authorize(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        resource: crate::models::common_models::Resource,
        right: crate::models::common_models::Right,
        id: String,
    ) -> std::result::Result<(), tonic::Status> {
        let project_id_result = match resource {
            crate::models::common_models::Resource::Project => Ok(id.clone()),
            crate::models::common_models::Resource::Dataset => {
                self.project_id_of_dataset(id.to_string().clone()).await
            }
            crate::models::common_models::Resource::DatasetVersion => {
                self.project_id_of_dataset_version(id.clone()).await
            }
            crate::models::common_models::Resource::ObjectGroup => {
                self.project_id_of_object_group(id.clone()).await
            }
            crate::models::common_models::Resource::Object => {
                self.project_id_of_object(id.clone()).await
            }
            crate::models::common_models::Resource::ObjectGroupRevision => {
                self.project_id_of_object_group_revision(id.clone()).await
            }
        };

        let project_id = match project_id_result {
            Ok(id) => id,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::unauthenticated(
                    "could not authorize requested action",
                ));
            }
        };

        let requested_rights = vec![right.clone()];

        if metadata.contains_key(USER_TOKEN_ENTRY_KEY) {
            return self.authorize_from_user_token(id, metadata, right).await;
        } else if metadata.contains_key(API_TOKEN_ENTRY_KEY) {
            return self
                .authorize_from_api_token(metadata, project_id, requested_rights)
                .await;
        }

        return Err(tonic::Status::unauthenticated(format!("could not find authentication token, please provide a token in metadata either with {} or {}", USER_TOKEN_ENTRY_KEY, API_TOKEN_ENTRY_KEY)));
    }

    async fn user_id(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> std::result::Result<String, tonic::Status> {
        if metadata.contains_key(USER_TOKEN_ENTRY_KEY) {
            return self.user_id_from_access_token(metadata).await;
        } else if metadata.contains_key(API_TOKEN_ENTRY_KEY) {
            return self.user_id_from_api_token(metadata).await;
        }

        return Err(tonic::Status::unauthenticated(format!("could not find authentication token, please provide a token in metadata either with {} or {}", USER_TOKEN_ENTRY_KEY, API_TOKEN_ENTRY_KEY)));
    }

    async fn project_id_from_api_token(
        &self,
        metadata: &MetadataMap,
    ) -> std::result::Result<APIToken, tonic::Status> {
        let token = match metadata.get(API_TOKEN_ENTRY_KEY) {
            Some(meta_token) => match meta_token.to_str() {
                Ok(token) => token,
                Err(e) => {
                    log::error!("{:?}", e);
                    return Err(tonic::Status::internal(format!("error decoding token")));
                }
            },
            None => {
                return Err(tonic::Status::unauthenticated(format!(
                    "could not find token {}",
                    API_TOKEN_ENTRY_KEY
                )));
            }
        };


        let query = doc! {
            "token": token
        };

        let db_token = match self
            .database_handler
            .find_one_by_key::<APIToken>(query)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::unauthenticated(
                    "could not authenticate from api_token",
                ));
            }
        };

        return Ok(db_token);
    }
}

#[derive(Debug)]
struct InvalidError {
    details: String,
}

#[allow(dead_code)]
impl InvalidError {
    fn new(msg: &str) -> InvalidError {
        InvalidError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for InvalidError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for InvalidError {
    fn description(&self) -> &str {
        &self.details
    }
}
