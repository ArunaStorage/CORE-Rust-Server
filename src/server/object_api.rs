use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_objects_service_server::DatasetObjectsService;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models,
    services::{CreateObjectGroupWithVersionRequest, GetObjectGroupVersionResponse},
};
use tonic::Response;

use crate::{
    auth::authenticator::AuthHandler,
    database::{
        common_models::{Resource, Right},
        database::{Database, ObjectGroupIDType},
        dataset_object_group::ObjectGroup,
    },
};
use crate::{
    database::dataset_object_group::ObjectGroupVersion,
    objectstorage::objectstorage::StorageHandler,
};

pub struct ObjectServer<T: Database + 'static> {
    pub mongo_client: Arc<T>,
    pub object_handler: Arc<dyn StorageHandler>,
    pub auth_handler: Arc<dyn AuthHandler>,
}

#[tonic::async_trait]
impl<'a, T: Database + 'static> DatasetObjectsService for ObjectServer<T> {
    async fn create_object_group_with_version(
        &self,
        request: tonic::Request<CreateObjectGroupWithVersionRequest>,
    ) -> Result<tonic::Response<GetObjectGroupVersionResponse>, tonic::Status> {
        let create_request = request.get_ref();
        let create_object_group_request = match &create_request.object_group {
            Some(value) => value,
            None => {
                return Err(tonic::Status::internal(
                    "create object group request required",
                ))
            }
        };

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::Dataset,
                Right::Write,
                create_object_group_request.dataset_id.clone(),
            )
            .await?;

        let object_group = match ObjectGroup::new_from_proto_create(
            &create_object_group_request,
            self.mongo_client.clone(),
        ) {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "could not create version ref for object group",
                ));
            }
        };

        let inserted_object_group = match self.mongo_client.store(object_group).await {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "could not create version ref for object group",
                ));
            }
        };

        let mut response = services::GetObjectGroupVersionResponse {
            object_group: Some(inserted_object_group.to_proto()),
            object_group_version: None,
        };

        let create_object_group_version_req = match &create_request.object_group_version {
            Some(value) => value,
            None => return Ok(Response::new(response)),
        };

        let object_group_version = match ObjectGroupVersion::new_from_proto_create(
            create_object_group_version_req,
            self.object_handler.get_bucket(),
            create_object_group_request.dataset_id.clone(),
            inserted_object_group.id.clone(),
            self.mongo_client.clone(),
        ) {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let inserted_group_version = match self.mongo_client.store(object_group_version).await {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        self.mongo_client.update_field::<ObjectGroup>(
            "id".to_string(),
            inserted_object_group.id.clone(),
            "head_id".to_string(),
            inserted_group_version.id.to_string(),
        );

        response.object_group_version = Some(inserted_group_version.to_proto());

        return Ok(Response::new(response));
    }

    async fn add_version_to_object_group(
        &self,
        request: tonic::Request<services::AddVersionToObjectGroupRequest>,
    ) -> Result<Response<GetObjectGroupVersionResponse>, tonic::Status> {
        let add_version_request = request.get_ref();
        let metadata = request.metadata();

        self.auth_handler
            .authorize(
                metadata,
                Resource::ObjectGroup,
                Right::Write,
                add_version_request.object_group_id.clone(),
            )
            .await?;

        let object_group_option: Option<Vec<ObjectGroup>> = match self
            .mongo_client
            .find_by_key(
                "id".to_string(),
                add_version_request.object_group_id.clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "error when getting object group from object group id",
                ));
            }
        };

        let object_group_vec = match object_group_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::internal(
                    "could not find object group of object group id",
                ));
            }
        };

        let object_group = &object_group_vec[0];

        let create_group_version_request = match &add_version_request.group_version {
            Some(value) => value,
            None => {
                return Err(tonic::Status::internal(
                    "could not find required CreateObjectGroupVersion field",
                ));
            }
        };

        let object_group_version = match ObjectGroupVersion::new_from_proto_create(
            create_group_version_request,
            self.object_handler.get_bucket(),
            object_group.dataset_id.clone(),
            add_version_request.object_group_id.clone(),
            self.mongo_client.clone(),
        ) {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let inserted_group_version = match self.mongo_client.store(object_group_version).await {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let get_object_group_response = GetObjectGroupVersionResponse {
            object_group: Some(object_group.to_proto()),
            object_group_version: Some(inserted_group_version.to_proto()),
        };

        self.mongo_client.update_field::<ObjectGroup>(
            "id".to_string(),
            add_version_request.object_group_id.clone(),
            "head_id".to_string(),
            inserted_group_version.id.to_string(),
        );

        return Ok(Response::new(get_object_group_response));
    }

    async fn get_object_group(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::GetObjectGroupVersionResponse>, tonic::Status> {
        let get_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroup,
                Right::Read,
                get_request.id.clone(),
            )
            .await?;

        let id = request.into_inner().id;

        let object_group_option: Option<Vec<ObjectGroup>> = match self
            .mongo_client
            .find_by_key("id".to_string(), id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(format!("{:?}", e)));
            }
        };

        let object_group = match object_group_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::not_found(format!(
                    "Could not find objectgroup with id: {}",
                    id.clone()
                )))
            }
        };

        let object = &object_group[0];

        let response = services::GetObjectGroupVersionResponse {
            object_group: Some(object.to_proto()),
            object_group_version: None,
        };

        Ok(Response::new(response))
    }

    async fn get_current_object_group(
        &self,
        request: tonic::Request<models::Id>,
    ) -> Result<Response<services::GetObjectGroupVersionResponse>, tonic::Status> {
        let id = request.get_ref();

        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroup,
                Right::Read,
                id.id.clone(),
            )
            .await?;

        let object_group_option: Option<ObjectGroup> = match self
            .mongo_client
            .find_one_by_key("id".to_string(), id.id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal("error on reading object group"));
            }
        };

        let object_group = match object_group_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::not_found(format!(
                    "Could not find objectgroup with id: {}",
                    id.id.clone()
                )))
            }
        };

        let object_group_version_option: Option<ObjectGroupVersion> = match self
            .mongo_client
            .find_one_by_key("id".to_string(), id.id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "error on reading object group version",
                ));
            }
        };

        let object_group_version = match object_group_version_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::not_found(format!(
                    "Could not find object group version with id: {}",
                    id.id.clone()
                )))
            }
        };

        let version_request = services::GetObjectGroupVersionResponse {
            object_group: Some(object_group.to_proto()),
            object_group_version: Some(object_group_version.to_proto()),
        };

        return Ok(Response::new(version_request));
    }

    async fn get_object_group_version(
        &self,
        request: tonic::Request<services::GetObjectGroupVersionRequest>,
    ) -> Result<Response<models::ObjectGroupVersion>, tonic::Status> {
        let get_request = request.get_ref();
        self.auth_handler
            .authorize(
                request.metadata(),
                Resource::ObjectGroupVersion,
                Right::Read,
                get_request.id.clone(),
            )
            .await?;

        let id = request.into_inner().id;


        let object_group_version_option: Option<ObjectGroupVersion> = match self
            .mongo_client
            .find_one_by_key("id".to_string(), id.clone())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(tonic::Status::internal(
                    "error on reading object group version",
                ));
            }
        };

        let object_group_version = match object_group_version_option {
            Some(value) => value,
            None => {
                return Err(tonic::Status::not_found(format!(
                    "Could not find object group version with id: {}",
                    id.clone()
                )))
            }
        };

        return Ok(Response::new(object_group_version.to_proto()));
    }

    async fn finish_object_upload(
        &self,
        _request: tonic::Request<models::Id>,
    ) -> Result<Response<models::Empty>, tonic::Status> {
        todo!()
    }
}
