use std::{error::Error, fmt, sync::Arc};

use mongodb::bson::doc;
use std::collections::HashSet;

use crate::database::{
    common_models::DatabaseModel,
    database::Database,
    dataset_model::DatasetEntry,
    dataset_object_group::{ObjectGroup, ObjectGroupRevision},
    dataset_version::DatasetVersion,
    project_model::ProjectEntry,
    apitoken::APIToken,
    common_models::Right,
};

use super::{authenticator::AuthHandler, oauth2_handler};
use async_trait::async_trait;

///Kind of token that has been found in the metadata
#[allow(dead_code)]
enum TokenType {
    OAuth2,
}

const API_TOKEN_ENTRY: &str = "API_TOKEN";

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

    async fn authorize_from_api_token(&self, token: &str, project_id: &str, requested_rights: Vec<Right>) -> Result<(), tonic::Status> {
        let query = doc! {
            "token": token
        };

        let db_token = match self.database_handler.find_one_by_key::<APIToken>(query).await?{
            Some(value ) => value,
            None => return Err(tonic::Status::unauthenticated("could not authenticate user from api token"))
        };
        
        if db_token.project_id != project_id {
            return Err(tonic::Status::permission_denied("could not authorize for requested project"))
        }

        let mut rights_hash_set = HashSet::new();
        for right in db_token.rights {
            rights_hash_set.insert(right);
        }

        for requested_right in requested_rights {
            if !rights_hash_set.contains(&requested_right) {
                return Err(tonic::Status::permission_denied("could not authorize for request project"))
            };
        }
        
        return Ok(())
    }

    async fn project_id_of_dataset(&self, id: String) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id
        };

        let dataset_option: Option<DatasetEntry> =
            self.database_handler.find_one_by_key(query).await?;

        let dataset = option_to_error(dataset_option, &id)?;

        return Ok(dataset.project_id);
    }

    async fn project_id_of_object_group(&self, id: String) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id,
        };

        let dataset_groups_option: Option<ObjectGroup> =
            self.database_handler.find_one_by_key(query).await?;

        let dataset_group = option_to_error(dataset_groups_option, &id)?;

        return self.project_id_of_dataset(dataset_group.id.clone()).await;
    }

    async fn project_id_of_object(&self, id: String) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id,
        };

        let dataset_group_option: Option<ObjectGroupRevision> =
            self.database_handler.find_one_by_key(query).await?;

        let dataset_group = option_to_error(dataset_group_option, &id)?;

        return self.project_id_of_dataset(dataset_group.id.clone()).await;
    }

    async fn project_id_of_dataset_version(&self, id: String) -> Result<String, tonic::Status> {
        let query = doc! {
            "id": &id,
        };

        let dataset_version_option: Option<DatasetVersion> =
            self.database_handler.find_one_by_key(query).await?;

        let dataset_version = option_to_error(dataset_version_option, &id)?;

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

        let dataset_groups_version_option: Option<ObjectGroupRevision> =
            self.database_handler.find_one_by_key(query).await?;

        let object_group_version = option_to_error(dataset_groups_version_option, &id)?;

        let project_id = self
            .project_id_of_dataset(object_group_version.datasete_id)
            .await?;
        Ok(project_id)
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
            crate::database::common_models::Resource::ObjectGroupRevision => {
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

        match metadata.get(API_TOKEN_ENTRY) {
            Some(meta_token) => match meta_token.to_str() {
                Ok(token) => self.authorize_from_api_token(token, project_id.as_str(), requested_rights).await?,
                Err(e) => {
                    log::error!("{:?}", e);
                    return Err(tonic::Status::unauthenticated(
                        "could not authorize requested action",
                    ));
                }
            },
            None => (),
        };

        let query = doc! {
            "id": &id,
        };

        let project_option: Option<ProjectEntry> =
            match self.database_handler.find_one_by_key(query).await {
                Ok(value) => value,
                Err(_) => {
                    return Err(tonic::Status::internal(
                        "could not authorize requested action",
                    ));
                }
            };
        let project = option_to_error(project_option, &project_id)?;

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

fn option_to_error<'de, T: DatabaseModel<'de>>(
    option: Option<T>,
    id: &str,
) -> std::result::Result<T, tonic::Status> {
    match option {
        Some(value) => return Ok(value),
        None => {
            let type_name = T::get_model_name().unwrap();
            return Err(tonic::Status::internal(format!(
                "could find {} with id {}",
                type_name, id
            )));
        }
    }
}
