use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::project_api_server::ProjectApi;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models::{self},
    services::DatasetList,
};
use tonic::Response;

use crate::database::{
    data_models::{DatasetEntry, ProjectEntry},
    database_model_wrapper::Database,
    mongo_connector::MongoHandler,
};

pub struct ProjectServer {
    pub mongo_client: Arc<MongoHandler>,
}

#[tonic::async_trait]
impl ProjectApi for ProjectServer {
    async fn create_project(
        &self,
        request: tonic::Request<services::CreateProjectRequest>,
    ) -> Result<tonic::Response<models::Project>, tonic::Status> {
        let project_model = match ProjectEntry::new_from_proto_create(request.into_inner()) {
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
        todo!()
    }

    async fn get_project_datasets(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<tonic::Response<services::DatasetList>, tonic::Status> {
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
        todo!()
    }

    async fn delete_project(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<tonic::Response<models::Empty>, tonic::Status> {
        todo!()
    }
}
