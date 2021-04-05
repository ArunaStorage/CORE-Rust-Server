use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_service_server::DatasetService;
use tonic::Response;

use crate::database::common_models::DatabaseHandler;
use crate::database::{
    data_models::DatasetEntry, database_model_wrapper::Database, mongo_connector::MongoHandler,
};

pub struct DatasetsServer {
    pub mongo_client: Arc<MongoHandler>,
}

#[tonic::async_trait]
impl DatasetService for DatasetsServer {
    async fn create_new_dataset(
        &self,
        request: tonic::Request<services::CreateDatasetRequest>,
    ) -> Result<Response<models::Dataset>, tonic::Status> {
        let dataset_model = match DatasetEntry::new_from_proto_create(request.into_inner()) {
            Ok(dataset) => dataset,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::invalid_argument(format!("{:?}", e)));
            }
        };

        let dataset = match self.mongo_client.store(dataset_model).await {
            Ok(dataset) => dataset,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::invalid_argument(format!("{:?}", e)));
            }
        };

        Ok(Response::new(dataset.to_proto_dataset()))
    }

    async fn dataset(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Dataset>, tonic::Status> {
        todo!()
    }

    async fn dataset_versions(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::DatasetVersionList>, tonic::Status> {
        todo!()
    }

    async fn dataset_object_groups(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::ObjectGroupList>, tonic::Status> {
        todo!()
    }

    async fn update_dataset_field(
        &self,
        request: tonic::Request<models::UpdateFieldsRequest>,
    ) -> Result<Response<models::Dataset>, tonic::Status> {
        todo!()
    }

    async fn delete_dataset(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Empty>, tonic::Status> {
        todo!()
    }

    async fn release_dataset_version(
        &self,
        request: tonic::Request<services::ReleaseDatasetVersionRequest>,
    ) -> Result<Response<models::DatasetVersion>, tonic::Status> {
        todo!()
    }

    async fn dataset_version_object_groups(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::ObjectGroupList>, tonic::Status> {
        todo!()
    }
}
