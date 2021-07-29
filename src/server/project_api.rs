use std::sync::Arc;

use crate::database::database::Database;
use crate::handler::common::HandlerWrapper;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::project_service_server::ProjectService;
use tonic::Response;

use crate::{
    auth::authenticator::AuthHandler,
    models::{
        common_models::{Resource, Right},
        dataset_model::DatasetEntry,
    },
};

/// Handles the project related API endpoints
/// The individual functions implemented are defined and documented in the API documentation
pub struct ProjectServer<T: Database + 'static> {
    pub handler: Arc<HandlerWrapper<T>>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<T: Database> ProjectService for ProjectServer<T> {
    async fn create_project(
        &self,
        request: tonic::Request<services::v1::CreateProjectRequest>,
    ) -> Result<tonic::Response<services::v1::CreateProjectResponse>, tonic::Status> {
        let user_id = self.auth_handler.user_id(request.metadata()).await?;

        let project = self
            .handler
            .create_handler
            .create_project(&request.into_inner(), user_id)
            .await?;

        let response = services::v1::CreateProjectResponse {
            project: project.id,
        };

        Ok(Response::new(response))
    }

    async fn add_user_to_project(
        &self,
        request: tonic::Request<services::v1::AddUserToProjectRequest>,
    ) -> Result<tonic::Response<services::v1::AddUserToProjectResponse>, tonic::Status> {
        let add_user = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Write,
                add_user.project_id.clone(),
            )
            .await?;

        self.handler
            .update_handler
            .add_user_to_project(add_user)
            .await?;

        let response = services::v1::AddUserToProjectResponse {};

        return Ok(tonic::Response::new(response));
    }

    async fn get_project_datasets(
        &self,
        request: tonic::Request<services::v1::GetProjectDatasetsRequest>,
    ) -> Result<tonic::Response<services::v1::GetProjectDatasetsResponse>, tonic::Status> {
        let get_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Read,
                get_request.id.clone(),
            )
            .await?;

        let datasets = self
            .handler
            .read_handler
            .read_from_parent_entry::<DatasetEntry>(get_request.id.as_str())
            .await?;
        let proto_datasets = datasets.into_iter().map(|x| x.to_proto_dataset()).collect();

        let dataset_list = services::v1::GetProjectDatasetsResponse {
            dataset: proto_datasets,
            ..Default::default()
        };

        return Ok(Response::new(dataset_list));
    }

    async fn get_user_projects(
        &self,
        request: tonic::Request<services::v1::GetUserProjectsRequest>,
    ) -> Result<tonic::Response<services::v1::GetUserProjectsResponse>, tonic::Status> {
        let _get_request = request.get_ref();
        let id = self.auth_handler.user_id(request.metadata()).await?;

        let projects = self
            .handler
            .read_handler
            .read_user_projects(id.as_str())
            .await?;
        let proto_projects = projects.into_iter().map(|x| x.to_proto_project()).collect();

        let project_list = services::v1::GetUserProjectsResponse {
            projects: proto_projects,
        };

        Ok(tonic::Response::new(project_list))
    }

    async fn delete_project(
        &self,
        request: tonic::Request<services::v1::DeleteProjectRequest>,
    ) -> Result<tonic::Response<services::v1::DeleteProjectResponse>, tonic::Status> {
        let _inner_request = request.get_ref();

        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn get_project(
        &self,
        request: tonic::Request<services::v1::GetProjectRequest>,
    ) -> Result<Response<services::v1::GetProjectResponse>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn create_api_token(
        &self,
        request: tonic::Request<services::v1::CreateApiTokenRequest>,
    ) -> Result<Response<services::v1::CreateApiTokenResponse>, tonic::Status> {
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

        let rights = vec![Right::Read, Right::Write];
        let inserted_token = self
            .handler
            .create_handler
            .create_api_token(user_id.as_str(), rights, get_request.id.as_str())
            .await?;

        let response = services::v1::CreateApiTokenResponse {
            token: Some(inserted_token.to_proto()),
        };

        return Ok(Response::new(response));
    }

    async fn get_api_token(
        &self,
        request: tonic::Request<services::v1::GetApiTokenRequest>,
    ) -> Result<Response<services::v1::GetApiTokenResponse>, tonic::Status> {
        let user_id = self.auth_handler.user_id(request.metadata()).await?;

        let proto_token = self
            .handler
            .read_handler
            .read_user_api_token(user_id.as_str())
            .await?
            .into_iter()
            .map(|x| x.to_proto())
            .collect();

        let reponse_token_list = services::v1::GetApiTokenResponse { token: proto_token };

        return Ok(Response::new(reponse_token_list));
    }

    async fn delete_api_token(
        &self,
        request: tonic::Request<services::v1::DeleteApiTokenRequest>,
    ) -> Result<Response<services::v1::DeleteApiTokenResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        unimplemented!();
    }
}
