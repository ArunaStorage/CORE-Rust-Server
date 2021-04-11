use std::sync::Arc;

use reqwest::get;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{models::Project, services};
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::project_api_server::ProjectApi;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models::{self},
    services::DatasetList,
};
use tonic::Response;

use crate::{auth::authenticator::AuthHandler, database::{common_models::{Resource, Right}, data_models::{DatasetEntry, ProjectEntry}, database_model_wrapper::Database}};

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
        
        let project_model = match ProjectEntry::new_from_proto_create(request.into_inner(), user_id) {
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
        self.auth_handler.authorize(request.metadata(), Resource::Project, Right::Write, add_user.project_id.clone()).await?;

        match self.mongo_client.add_user(add_user).await{
            Ok(_) => {}
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("error while adding user"));
            }
        };

        let project_option: Option<Vec<ProjectEntry>> = match self.mongo_client.find_by_key("id".to_string(), add_user.project_id.clone()).await {
            Ok(value) => {value}
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("error while adding user"));
            }
        };

        let projects = project_option.unwrap();

        let project_entry = projects[0].clone();
        return Ok(tonic::Response::new(project_entry.to_proto_project()));
    }

    async fn get_project_datasets(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<tonic::Response<services::DatasetList>, tonic::Status> {
        let get_request = request.get_ref();
        self.auth_handler.authorize(request.metadata(), Resource::Project, Right::Read, get_request.id.clone()).await?;

        let datasets_option: Option<Vec<DatasetEntry>> = match self
            .mongo_client
            .find_by_key("project_id".to_string(), request.into_inner().id)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let datasets = match datasets_option {
            Some(datasets) => datasets,
            None => return Ok(Response::new(DatasetList::default())),
        };

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
        let get_request = request.get_ref();
        let id = self.auth_handler.user_id(request.metadata()).await?;

        let projects: Option<Vec<ProjectEntry>> = match self.mongo_client.find_by_key("users.user_id".to_string(), id).await{
            Ok(value) => {value}
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("could not read user projects"));
            }
        };

        let entries = match projects {
            Some(value) => {value}
            None => {Vec::new()}
        };

        let mut proto_entries: Vec<Project> = Vec::new();

        for entry in entries {
            proto_entries.push(entry.to_proto_project())
        }

        let project_list = services::ProjectList{
            projects: proto_entries,
        };


        Ok(tonic::Response::new(project_list))
    }

    async fn delete_project(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<tonic::Response<models::Empty>, tonic::Status> {
        todo!()
    }
}
