use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::dataset_service_server::DatasetService;
use tonic::Response;

use crate::database::database::Database;
use crate::handler::common::HandlerWrapper;
use crate::{
    auth::authenticator::AuthHandler,
    models::{
        common_models::{Resource, Right},
        dataset_model::DatasetEntry,
        dataset_object_group::ObjectGroup,
        dataset_version::DatasetVersion,
    },
};

pub struct DatasetsServer<T: Database + 'static> {
    pub handler_wrapper: Arc<HandlerWrapper<T>>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<T: Database> DatasetService for DatasetsServer<T> {
    async fn create_dataset(
        &self,
        request: tonic::Request<services::v1::CreateDatasetRequest>,
    ) -> Result<Response<services::v1::CreateDatasetResponse>, tonic::Status> {
        let inner_request = request.get_ref();

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Write,
                inner_request.project_id.clone(),
            )
            .await?;

        let dataset = self
            .handler_wrapper
            .create_handler
            .create_dataset(inner_request)
            .await?;

        let response = services::v1::CreateDatasetResponse { id: dataset.id };

        return Ok(Response::new(response));
    }

    async fn get_dataset(
        &self,
        request: tonic::Request<services::v1::GetDatasetRequest>,
    ) -> Result<Response<services::v1::GetDatasetResponse>, tonic::Status> {
        let inner_request = request.get_ref();

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let dataset = self
            .handler_wrapper
            .read_handler
            .read_entry_by_id::<DatasetEntry>(inner_request.id.as_str())
            .await?;

        let response = services::v1::GetDatasetResponse {
            dataset: vec![dataset.to_proto_dataset()],
        };

        return Ok(Response::new(response));
    }

    async fn get_dataset_versions(
        &self,
        request: tonic::Request<services::v1::GetDatasetVersionsRequest>,
    ) -> Result<Response<services::v1::GetDatasetVersionsResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let dataset_versions = self
            .handler_wrapper
            .read_handler
            .read_from_parent_entry::<DatasetVersion>(inner_request.id.as_str())
            .await?;
        let dataset_versions_proto = dataset_versions.into_iter().map(|x| x.to_proto()).collect();

        let version_list = services::v1::GetDatasetVersionsResponse {
            dataset_versions: dataset_versions_proto,
        };

        return Ok(Response::new(version_list));
    }

    async fn get_dataset_object_groups(
        &self,
        request: tonic::Request<services::v1::GetDatasetObjectGroupsRequest>,
    ) -> Result<Response<services::v1::GetDatasetObjectGroupsResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let object_groups: Vec<ObjectGroup> = self
            .handler_wrapper
            .read_handler
            .read_from_parent_entry(inner_request.id.as_str())
            .await?;
        let object_groups_proto = object_groups.into_iter().map(|x| x.to_proto()).collect();

        let object_groups_list = services::v1::GetDatasetObjectGroupsResponse {
            object_groups: object_groups_proto,
        };

        return Ok(Response::new(object_groups_list));
    }

    async fn get_current_object_group_revisions(
        &self,
        _request: tonic::Request<services::v1::GetCurrentObjectGroupRevisionsRequest>,
    ) -> Result<Response<services::v1::GetCurrentObjectGroupRevisionsResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn update_dataset_field(
        &self,
        request: tonic::Request<services::v1::UpdateDatasetFieldRequest>,
    ) -> Result<Response<services::v1::UpdateDatasetFieldResponse>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn delete_dataset(
        &self,
        request: tonic::Request<services::v1::DeleteDatasetRequest>,
    ) -> Result<Response<services::v1::DeleteDatasetResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        self.handler_wrapper
            .delete_handler
            .delete_dataset(inner_request.id.clone())
            .await?;

        return Ok(Response::new(services::v1::DeleteDatasetResponse {}));
    }

    async fn release_dataset_version(
        &self,
        request: tonic::Request<services::v1::ReleaseDatasetVersionRequest>,
    ) -> Result<Response<services::v1::ReleaseDatasetVersionResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Write,
                inner_request.dataset_id.clone(),
            )
            .await?;

        let mut poll_authz_queue = FuturesUnordered::new();
        for revision_id in inner_request.revision_ids.clone() {
            let authz_request = self.auth_handler.authorize(
                request.metadata(),
                Resource::ObjectGroupRevision,
                Right::Write,
                revision_id,
            );

            poll_authz_queue.push(authz_request);

            if poll_authz_queue.len() == 100 {
                poll_authz_queue.next().await.unwrap()?;
            }
        }

        while let Some(value) = poll_authz_queue.next().await {
            value?
        }

        let response = services::v1::ReleaseDatasetVersionResponse {};

        return Ok(Response::new(response));
    }

    async fn get_datset_version_revisions(
        &self,
        request: tonic::Request<services::v1::GetDatsetVersionRevisionsRequest>,
    ) -> Result<Response<services::v1::GetDatsetVersionRevisionsResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::DatasetVersion,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;
        unimplemented!()
    }

    async fn get_dataset_version(
        &self,
        request: tonic::Request<services::v1::GetDatasetVersionRequest>,
    ) -> Result<Response<services::v1::GetDatasetVersionResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::DatasetVersion,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        let dataset_version = self
            .handler_wrapper
            .read_handler
            .read_entry_by_id::<DatasetVersion>(inner_request.id.as_str())
            .await?;

        let response = services::v1::GetDatasetVersionResponse {
            dataset_version: Some(dataset_version.to_proto()),
        };

        return Ok(Response::new(response));
    }

    async fn delete_dataset_version(
        &self,
        request: tonic::Request<services::v1::DeleteDatasetVersionRequest>,
    ) -> Result<Response<services::v1::DeleteDatasetVersionResponse>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::DatasetVersion,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        self.handler_wrapper
            .delete_handler
            .delete_dataset_version(inner_request.id.clone())
            .await?;

        return Ok(Response::new(services::v1::DeleteDatasetVersionResponse {}));
    }
}
