use std::sync::Arc;

use crate::server::util;
use mongodb::bson::doc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models::{self};
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::project_api_server::ProjectApi;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{models::Project, services};
use tonic::Response;

use crate::{
    auth::authenticator::AuthHandler,
    database::{
        common_models::{Resource, Right},
        database::Database,
        dataset_model::DatasetEntry,
        project_model::ProjectEntry,
        apitoken::APIToken,
    },
};

/// Handles the project related API endpoints
/// The individual functions implemented are defined and documented in the API documentation
pub struct ProjectServer<T: Database + 'static> {
    pub mongo_client: Arc<T>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<T: Database> ProjectApi for ProjectServer<T> {
    async fn create_project(
        &self,
        request: tonic::Request<services::CreateProjectRequest>,
    ) -> Result<tonic::Response<models::Project>, tonic::Status> {
        let user_id = self.auth_handler.user_id(request.metadata()).await?;

        let project_model = match ProjectEntry::new_from_proto_create(request.into_inner(), user_id)
        {
            Ok(project) => project,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::invalid_argument(format!("{:?}", e)));
            }
        };

        let project = match self.mongo_client.store(project_model).await {
            Ok(project) => project,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        Ok(Response::new(project.to_proto_project()))
    }

    async fn add_user_to_project(
        &self,
        request: tonic::Request<services::AddUserToProjectRequest>,
    ) -> Result<tonic::Response<models::Project>, tonic::Status> {
        let add_user = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Write,
                add_user.project_id.clone(),
            )
            .await?;

        match self.mongo_client.add_user(add_user).await {
            Ok(_) => {}
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("error while adding user"));
            }
        };

        let query = doc! {
            "id": add_user.project_id.as_str()
        };

        let project_option: Option<ProjectEntry> = self.mongo_client.find_one_by_key(query).await?;

        let project =
            util::tonic_error_if_value_not_found(&project_option, add_user.project_id.as_str())?;

        return Ok(tonic::Response::new(project.to_proto_project()));
    }

    async fn get_project_datasets(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<tonic::Response<services::DatasetList>, tonic::Status> {
        let get_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Read,
                get_request.id.clone(),
            )
            .await?;

        let query = doc! {
            "project_id": get_request.id.as_str()
        };

        let datasets: Vec<DatasetEntry> = self.mongo_client.find_by_key(query).await?;

        let mut proto_datasets = Vec::new();
        for entry in datasets {
            let proto_dataset = entry.to_proto_dataset();
            proto_datasets.push(proto_dataset)
        }

        let dataset_list = services::DatasetList {
            dataset: proto_datasets,
            ..Default::default()
        };

        return Ok(Response::new(dataset_list));
    }

    async fn get_user_projects(
        &self,
        request: tonic::Request<models::Empty>,
    ) -> Result<tonic::Response<services::ProjectList>, tonic::Status> {
        let _get_request = request.get_ref();
        let id = self.auth_handler.user_id(request.metadata()).await?;

        let query = doc! {
            "users.user_id": id.as_str()
        };

        let projects: Vec<ProjectEntry> = self.mongo_client.find_by_key(query).await?;

        let mut proto_entries: Vec<Project> = Vec::new();

        for project in projects {
            proto_entries.push(project.to_proto_project())
        }

        let project_list = services::ProjectList {
            projects: proto_entries,
        };

        Ok(tonic::Response::new(project_list))
    }

    async fn delete_project(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<tonic::Response<models::Empty>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn get_project(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<Project>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn create_api_token(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::ApiToken>, tonic::Status> {
        let get_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Write,
                get_request.id.clone(),
            )
            .await?;

        let user_id = self.auth_handler.user_id(request.metadata()).await?;
        let rights = vec![Right::Write, Right::Read];
        let api_token = APIToken::new(user_id, rights, get_request.id.clone())?;

        let inserted_token = self.mongo_client.store(api_token).await?;

        return Ok(Response::new(inserted_token.to_proto()));
    }

    async fn get_api_token(
        &self,
        request: tonic::Request<models::Empty>,
    ) -> Result<Response<services::ApiTokenList>, tonic::Status> {
        let user_id = self.auth_handler.user_id(request.metadata()).await?;

        let query = doc! {
            "user_id": user_id
        };

        let token_list = self.mongo_client.find_by_key::<APIToken>(query).await?;
        let mut proto_token = Vec::new();
        for token in token_list {
            proto_token.push(token.to_proto())
        };

        let reponse_token_list = services::ApiTokenList{
            token: proto_token
        };

        return Ok(Response::new(reponse_token_list));
    }

    async fn delete_api_token(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Empty>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }
}
