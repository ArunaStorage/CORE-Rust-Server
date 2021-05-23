use std::sync::Arc;

use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;

use mongodb::bson::doc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models::{self};
use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_service_server::DatasetService;
use services::DatasetVersionList;
use tonic::Response;

use log::error;

use crate::{
    auth::authenticator::AuthHandler,
    database::{
        common_models::{Resource, Right},
        database::Database,
        dataset_model::DatasetEntry,
        dataset_object_group::{ObjectGroup, ObjectGroupRevision},
        dataset_version::DatasetVersion,
    },
};

pub struct DatasetsServer<T: Database + 'static> {
    pub database_client: Arc<T>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<T: Database> DatasetService for DatasetsServer<T> {
    async fn create_dataset(
        &self,
        request: tonic::Request<services::CreateDatasetRequest>,
    ) -> Result<Response<models::Dataset>, tonic::Status> {
        let inner_request = request.get_ref();

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Write,
                inner_request.project_id.clone(),
            )
            .await?;

        let dataset = DatasetEntry::new_from_proto_create(inner_request)?;

        let inserted_dataset = self.database_client.store(dataset).await?;
        return Ok(Response::new(inserted_dataset.to_proto_dataset()));
    }

    async fn get_dataset(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Dataset>, tonic::Status> {
        let inner_request = request.get_ref();

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Project,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let query = doc! {};

        let dataset: DatasetEntry = match self.database_client.find_one_by_key(query).await? {
            Some(value) => value,
            None => {
                let e = tonic::Status::not_found("could not find request dataset");
                error!("{:?}", e);
                return Err(e);
            }
        };

        return Ok(Response::new(dataset.to_proto_dataset()));
    }

    async fn get_dataset_versions(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::DatasetVersionList>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let query = doc! {
            "dataset_id": inner_request.id.as_str()
        };

        let dataset_versions: Vec<DatasetVersion> = self.database_client.find_by_key(query).await?;

        let mut proto_versions = Vec::new();
        for dataset_version in dataset_versions {
            let proto_version = dataset_version.to_proto()?;
            proto_versions.push(proto_version);
        }

        let version_list_response = DatasetVersionList {
            dataset_version: proto_versions,
        };

        return Ok(Response::new(version_list_response));
    }

    async fn get_dataset_object_groups(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::ObjectGroupList>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let query = doc! {
            "dataset_id": inner_request.id.as_str()
        };

        let object_groups: Vec<ObjectGroup> = self.database_client.find_by_key(query).await?;

        let mut proto_groups = Vec::new();
        for object_group in object_groups {
            let proto_version = object_group.to_proto()?;
            proto_groups.push(proto_version);
        }

        let version_list_response = services::ObjectGroupList {
            object_groups: proto_groups,
        };

        return Ok(Response::new(version_list_response));
    }

    async fn get_current_object_group_revisions(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::ObjectGroupRevisions>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Read,
                inner_request.id.clone(),
            )
            .await?;

        let query = doc! {
            "dataset_id": inner_request.id.as_str()
        };

        let object_groups: Vec<ObjectGroup> = self.database_client.find_by_key(query).await?;

        let mut object_group_revisions = Vec::new();

        for group_chunk in object_groups.chunks(100) {
            let mut db_requests = Vec::new();
            for group in group_chunk {
                let query = doc! {
                    "dataset_id": inner_request.id.as_str(),
                    "revision": group.revision_counter,
                };

                let request_future = self
                    .database_client
                    .find_one_by_key::<ObjectGroupRevision>(query);
                db_requests.push(request_future);
            }

            let results = join_all(db_requests).await;

            for result in results {
                let value = match result? {
                    Some(value) => value,
                    None => {
                        let e = tonic::Status::not_found(format!(
                            "could not find expected object group revision"
                        ));
                        error!("{:?}", e);
                        return Err(e);
                    }
                };

                object_group_revisions.push(value.to_proto());
            }
        }

        let group_revisions = services::ObjectGroupRevisions {
            object_group_revision: object_group_revisions,
        };

        Ok(Response::new(group_revisions))
    }

    async fn update_dataset_field(
        &self,
        request: tonic::Request<models::UpdateFieldsRequest>,
    ) -> Result<Response<models::Dataset>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn delete_dataset(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Empty>, tonic::Status> {
        let _inner_request = request.get_ref();
        return Err(tonic::Status::unimplemented("not implemented"));
    }

    async fn release_dataset_version(
        &self,
        request: tonic::Request<services::ReleaseDatasetVersionRequest>,
    ) -> Result<Response<models::DatasetVersion>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Write,
                inner_request.dataset_id.clone(),
            )
            .await?;

        let dataset_version = DatasetVersion::new_from_proto_create(inner_request)?;
        let inserted_dataset_version = self.database_client.store(dataset_version).await?;

        let mut poll_authz_queue = FuturesUnordered::new();
        for revision_id in inner_request.revision_ids.clone() {
            let authz_request = self.auth_handler.authorize(
                request.metadata(),
                Resource::ObjectGroupRevision,
                Right::Write,
                revision_id,
            );

            poll_authz_queue.push(authz_request);
        }

        if poll_authz_queue.len() == 100 {
            poll_authz_queue.next().await.unwrap()?;
        }

        while let Some(value) = poll_authz_queue.next().await {
            value?
        }

        //TODO: Check if using multiple
        let mut poll_revision_version_add = FuturesUnordered::new();
        for revision_id in inner_request.revision_ids.clone() {
            let query = doc! {
                "id": revision_id
            };

            let update = doc! {
                "$addToSet": {
                    "dataset_versions": inserted_dataset_version.id.clone()
                }
            };

            let update_request = self
                .database_client
                .update_field::<ObjectGroupRevision>(query, update);
            poll_revision_version_add.push(update_request);

            if poll_revision_version_add.len() == 100 {
                poll_revision_version_add.next().await.unwrap()?;
            }
        }

        while let Some(value) = poll_revision_version_add.next().await {
            value?;
        }

        return Ok(Response::new(inserted_dataset_version.to_proto()?));
    }

    async fn get_datset_version_revisions(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::ObjectGroupRevisions>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::DatasetVersion,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        let query = doc! {
            "id": inner_request.id.clone()
        };

        let _dataset_version = match self
            .database_client
            .find_one_by_key::<DatasetVersion>(query)
            .await?
        {
            Some(value) => value,
            None => {
                let e = tonic::Status::not_found(format!(
                    "could not find expected dataset version with version: {}",
                    inner_request.id.clone()
                ));
                error!("{:?}", e);
                return Err(e);
            }
        };

        let query = doc! {
            "dataset_versions": inner_request.id.clone()
        };

        let revisions = self
            .database_client
            .find_by_key::<ObjectGroupRevision>(query)
            .await?;

        let mut proto_revisions = Vec::new();
        for revision in revisions {
            proto_revisions.push(revision.to_proto())
        }

        let revisions_list = services::ObjectGroupRevisions {
            object_group_revision: proto_revisions,
        };

        Ok(Response::new(revisions_list))
    }

    async fn get_dataset_version(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<models::DatasetVersion>, tonic::Status> {
        let inner_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::DatasetVersion,
                Right::Write,
                inner_request.id.clone(),
            )
            .await?;

        let query = doc! {
            "id": inner_request.id.clone()
        };

        let dataset_version = match self
            .database_client
            .find_one_by_key::<DatasetVersion>(query)
            .await?
        {
            Some(value) => value,
            None => {
                let e = tonic::Status::not_found(format!(
                    "could not find expected dataset version with version: {}",
                    inner_request.id.clone()
                ));
                error!("{:?}", e);
                return Err(e);
            }
        };

        return Ok(Response::new(dataset_version.to_proto()?));
    }

    async fn delete_dataset_version(
        &self,
        _request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Empty>, tonic::Status> {
        todo!()
    }
}
