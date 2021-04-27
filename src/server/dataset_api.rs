use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_service_server::DatasetService;
use tonic::Response;

use crate::{
    auth::authenticator::AuthHandler,
    database::{
        common_models::{Resource, Right},
        database::Database,
        dataset_model::DatasetEntry,
        dataset_object_group::ObjectGroup,
        dataset_version::DatasetVersion,
    },
};

pub struct DatasetsServer<T: Database + 'static> {
    pub mongo_client: Arc<T>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<T: Database> DatasetService for DatasetsServer<T> {
    async fn create_new_dataset(
        &self,
        request: tonic::Request<services::CreateDatasetRequest>,
    ) -> Result<Response<models::Dataset>, tonic::Status> {
        let create_request = request.get_ref();

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Write,
                create_request.project_id.clone(),
            )
            .await?;

        let dataset_model = match DatasetEntry::new_from_proto_create(create_request.clone()) {
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
        let get_dataset = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Read,
                get_dataset.id.clone(),
            )
            .await?;

        let dataset_find_result: Option<DatasetEntry> = match self
            .mongo_client
            .find_one_by_key("id".to_string(), get_dataset.id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let dataset = match dataset_find_result {
            Some(value) => value,
            None => {
                return Err(tonic::Status::invalid_argument(format!(
                    "Could not find dataset entry with id: {:?}",
                    get_dataset.id.clone()
                )));
            }
        };

        return Ok(Response::new(dataset.to_proto_dataset()));
    }

    async fn dataset_versions(
        &self,
        _request: tonic::Request<models::Id>,
    ) -> Result<Response<services::DatasetVersionList>, tonic::Status> {
        todo!()
    }

    async fn dataset_object_groups(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::ObjectGroupList>, tonic::Status> {
        let get_groups_req = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Read,
                get_groups_req.id.clone(),
            )
            .await?;

        let object_groups: Vec<ObjectGroup> = match self
            .mongo_client
            .find_by_key("dataset_id".to_string(), get_groups_req.id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "Could not read object groups of dataset {}",
                    get_groups_req.id.clone()
                )));
            }
        };

        let mut proto_object_groups = Vec::new();
        for object_group in object_groups {
            let proto_object_group = object_group.to_proto();
            proto_object_groups.push(proto_object_group);
        }

        let object_group_list = services::ObjectGroupList{
            object_groups: proto_object_groups,
        };

        Ok(Response::new(object_group_list))
    }

    async fn update_dataset_field(
        &self,
        _request: tonic::Request<models::UpdateFieldsRequest>,
    ) -> Result<Response<models::Dataset>, tonic::Status> {
        todo!()
    }

    async fn delete_dataset(
        &self,
        _request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Empty>, tonic::Status> {
        todo!()
    }

    async fn release_dataset_version(
        &self,
        request: tonic::Request<services::ReleaseDatasetVersionRequest>,
    ) -> Result<Response<models::DatasetVersion>, tonic::Status> {
        let relese_version_create = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Write,
                relese_version_create.dataset_id.clone(),
            )
            .await?;

        let model = match DatasetVersion::new_from_proto_create(request.into_inner()) {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("Could not create dataset version"));
            }
        };

        let version = match self.mongo_client.store(model).await {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("Could not create dataset version"));
            }
        };

        let version_proto = match version.to_proto() {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("Could not create dataset version"));
            }
        };

        return Ok(Response::new(version_proto));
    }

    async fn dataset_version_object_groups(
        &self,
        _request: tonic::Request<models::Id>,
    ) -> Result<Response<services::ObjectGroupList>, tonic::Status> {
        todo!()
    }
}
