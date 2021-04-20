use std::{error::Error, fmt, sync::Arc};

use crate::database::{database_model_wrapper::Database, dataset_model::DatasetEntry, dataset_object_group::DatasetObjectGroup, dataset_version::DatasetVersion, project_model::ProjectEntry};

use super::{authenticator::AuthHandler, oauth2_handler};
use async_trait::async_trait;

///Kind of token that has been found in the metadata
enum TokenType {
    OAuth2,
}

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

    async fn project_id_of_dataset(&self, id: String) -> ResultWrapper<String> {
        let datasets_option: Option<Vec<DatasetEntry>> = self
            .database_handler
            .find_by_key("id".to_string(), id.clone())
            .await?;
        let datasets = match datasets_option {
            Some(value) => value,
            None => {
                return Err::<String, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                    InvalidError::new(&format!(
                        "Could not find dataset with id: {}",
                        id.to_string()
                    )),
                ));
            }
        };

        let dataset = datasets[0].clone();

        return Ok(dataset.project_id);
    }

    async fn project_id_of_object_group(&self, id: String) -> ResultWrapper<String> {
        let dataset_groups_option: Option<Vec<DatasetObjectGroup>> = self
            .database_handler
            .find_by_key("id".to_string(), id.clone())
            .await?;
        let dataset_group = match dataset_groups_option {
            Some(value) => value,
            None => {
                return Err::<String, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                    InvalidError::new(&format!(
                        "Could not find datasetobjectgroup with id: {}",
                        id.to_string()
                    )),
                ));
            }
        };

        let dataset_group = dataset_group[0].clone();

        return self.project_id_of_dataset(dataset_group.id.clone()).await;
    }

    async fn project_id_of_object(&self, id: String) -> ResultWrapper<String> {
        let dataset_groups_option: Option<Vec<DatasetObjectGroup>> = self
            .database_handler
            .find_by_key("objects.id".to_string(), id.clone())
            .await?;
        let dataset_group = match dataset_groups_option {
            Some(value) => value,
            None => {
                return Err::<String, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                    InvalidError::new(&format!(
                        "Could not find datasetobjectgroup with id: {}",
                        id.to_string()
                    )),
                ));
            }
        };

        let dataset_group = dataset_group[0].clone();

        return self.project_id_of_dataset(dataset_group.id.clone()).await;
    }

    async fn project_id_of_dataset_version(&self, id: String) -> ResultWrapper<String> {
        let dataset_groups_option: Option<Vec<DatasetVersion>> = self
            .database_handler
            .find_by_key("objects.id".to_string(), id.clone())
            .await?;
        let dataset_group = match dataset_groups_option {
            Some(value) => value,
            None => {
                return Err::<String, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                    InvalidError::new(&format!(
                        "Could not find datasetobjectgroup with id: {}",
                        id.to_string()
                    )),
                ));
            }
        };

        let dataset_group = dataset_group[0].clone();

        return self.project_id_of_dataset(dataset_group.id.clone()).await;
    }
}

#[async_trait]
impl<T: Database> AuthHandler for ProjectAuthzHandler<T> {
    async fn authorize(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        resource: crate::database::common_models::Resource,
        right: crate::database::common_models::Right,
        id: String,
    ) -> std::result::Result<(), tonic::Status> {
        let project_id_result = match resource {
            crate::database::common_models::Resource::Project => Ok(id.clone()),
            crate::database::common_models::Resource::Dataset => {
                self.project_id_of_dataset(id.to_string().clone()).await
            }
            crate::database::common_models::Resource::DatasetVersion => {
                self.project_id_of_dataset_version(id.clone()).await
            }
            crate::database::common_models::Resource::ObjectGroup => {
                self.project_id_of_object_group(id.clone()).await
            }
            crate::database::common_models::Resource::Object => {
                self.project_id_of_object(id.clone()).await
            }
        };

        let project_id = match project_id_result {
            Ok(id) => id,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "could not authorize requested action",
                ));
            }
        };

        let project_option: Option<Vec<ProjectEntry>> = match self
            .database_handler
            .find_by_key("id".to_string(), project_id)
            .await
        {
            Ok(value) => value,
            Err(_) => {
                return Err(tonic::Status::internal(
                    "could not authorize requested action",
                ));
            }
        };
        let project = match project_option {
            Some(value) => value[0].clone(),
            None => {
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

        return Err(tonic::Status::internal(
            "could not authorize requested action",
        ));
    }

    async fn user_id(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> std::result::Result<String, tonic::Status> {
        let access_token = match metadata.get("AccessToken") {
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
}

#[derive(Debug)]
struct InvalidError {
    details: String,
}

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
