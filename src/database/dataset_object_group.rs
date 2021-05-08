use std::sync::Arc;

use chrono::{Timelike, Utc};
use mongodb::bson::DateTime;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{models, services};
use serde::{Deserialize, Serialize};

use super::{
    common_models::{
        to_labels, to_metadata, to_proto_labels, to_proto_metadata, to_proto_status, DatabaseModel,
        Label, Location, Metadata, Origin, Status, Version,
    },
    database::Database,
};

use super::common_models;

/// Here are all models that are used to store object related components
/// A ObjectGroupVersions is used to keep track of the history of a set of DatasetObjectGroups

/// Stores the history of object groups
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct ObjectGroup {
    pub id: String,
    pub name: String,
    pub dataset_id: String,
    pub labels: Vec<Label>,
    pub metadata: Vec<Metadata>,
    pub status: Status,
    pub head_id: String,
    pub revision_counter: i64,
}

impl DatabaseModel<'_> for ObjectGroup {
    fn get_model_name() -> Result<String, tonic::Status> {
        Ok("ObjectGroup".to_string())
    }
}

impl ObjectGroup {
    pub fn new_from_proto_create<T: Database>(
        request: &services::CreateObjectGroupRequest,
        _handler: Arc<T>,
    ) -> Result<Self, tonic::Status> {
        let uuid = uuid::Uuid::new_v4();

        let object_group = ObjectGroup {
            id: uuid.to_string(),
            name: request.name.clone(),
            labels: to_labels(&request.labels),
            dataset_id: request.dataset_id.clone(),
            status: Status::Initializing,
            metadata: to_metadata(&request.metadata),
            revision_counter: 0,
            ..Default::default()
        };

        return Ok(object_group);
    }

    pub fn to_proto(&self) -> Result<models::ObjectGroup, tonic::Status> {
        let proto_object = models::ObjectGroup {
            id: self.id.clone(),
            dataset_id: self.dataset_id.clone(),
            labels: to_proto_labels(&self.labels),
            metadata: to_proto_metadata(&self.metadata),
            head_id: self.head_id.clone(),
            name: self.name.clone(),
            status: to_proto_status(&self.status) as i32,
            current_revision: self.revision_counter,
        };

        return Ok(proto_object);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectGroupRevision {
    pub id: String,
    pub datasete_id: String,
    pub object_group_id: String,
    pub date_create: DateTime,
    pub labels: Vec<Label>,
    pub metadata: Vec<Metadata>,
    pub objects_count: i64,
    pub objects: Vec<DatasetObject>,
    pub version: Version,
    pub revision: i64,
    pub dataset_versions: Vec<String>,
}

impl DatabaseModel<'_> for ObjectGroupRevision {
    fn get_model_name() -> Result<String, tonic::Status> {
        Ok("ObjectGroupVersion".to_string())
    }
}

impl ObjectGroupRevision {
    pub fn new_from_proto_create(
        request: &services::CreateObjectGroupRevisionRequest,
        object_group: &ObjectGroup,
        bucket: String,
    ) -> Result<Self, tonic::Status> {
        let uuid = uuid::Uuid::new_v4();

        let timestamp = Utc::now();

        let mut objects = Vec::new();

        for create_object_request in &request.objects {
            let object = DatasetObject::new_from_proto_create(
                &create_object_request,
                object_group.dataset_id.clone(),
                bucket.clone(),
            )?;
            objects.push(object);
        }

        let objects_count = objects.len().clone();

        let object_group = ObjectGroupRevision {
            id: uuid.to_string(),
            labels: to_labels(&request.labels),
            metadata: to_metadata(&request.metadata),
            datasete_id: object_group.dataset_id.clone(),
            date_create: DateTime::from(timestamp),
            objects: objects,
            objects_count: objects_count as i64,
            object_group_id: object_group.id.clone(),
            version: Default::default(),
            revision: object_group.revision_counter,
            dataset_versions: Vec::new(),
        };

        return Ok(object_group);
    }

    pub fn to_proto(&self) -> models::ObjectGroupRevision {
        let mut proto_objects = Vec::new();

        for object in &self.objects {
            let proto_object = object.to_proto_object();
            proto_objects.push(proto_object);
        }

        let proto_object = models::ObjectGroupRevision {
            id: self.id.clone(),
            dataset_id: self.datasete_id.clone(),
            labels: to_proto_labels(&self.labels),
            metadata: to_proto_metadata(&self.metadata),
            objects: proto_objects,
            object_group_id: self.object_group_id.clone(),
            revision: self.revision,
            ..Default::default()
        };

        return proto_object;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DatasetObject {
    pub id: String,
    pub filename: String,
    pub filetype: String,
    pub origin: Origin,
    pub content_len: i64,
    pub location: Location,
    pub created: DateTime,
    pub metadata: Vec<Metadata>,
    pub upload_id: String,
}

impl DatabaseModel<'_> for DatasetObject {
    fn get_model_name() -> Result<String, tonic::Status> {
        Ok("Object".to_string())
    }
}

impl DatasetObject {
    pub fn new_from_proto_create(
        request: &services::CreateObjectRequest,
        dataset_id: String,
        bucket: String,
    ) -> Result<Self, tonic::Status> {
        let timestamp = Utc::now();
        let uuid = uuid::Uuid::new_v4();

        let object_key = format!(
            "{}/{}/{}",
            dataset_id,
            uuid.to_string().clone(),
            request.filename.clone()
        );

        let location = Location {
            bucket: bucket,
            key: object_key,
            index_location: common_models::IndexLocation {
                start_byte: 0,
                end_byte: 0,
            },
            location_type: common_models::LocationType::Object,
            url: "".to_string(),
        };

        let object = DatasetObject {
            id: uuid.to_string().clone(),
            filename: request.filename.clone(),
            filetype: request.filetype.clone(),
            origin: Origin::default(),
            content_len: request.content_len,
            location: location,
            created: DateTime::from(timestamp),
            upload_id: "".to_string(),
            metadata: to_metadata(&request.metadata),
        };

        Ok(object)
    }

    pub fn to_proto_object(&self) -> models::Object {
        let timestamp = prost_types::Timestamp {
            seconds: self.created.timestamp(),
            nanos: self.created.nanosecond() as i32,
        };

        let proto_object = models::Object {
            id: self.id.clone(),
            filename: self.filename.clone(),
            filetype: self.filetype.clone(),
            content_len: self.content_len,
            created: Some(timestamp),
            upload_id: self.upload_id.clone(),
            metadata: to_proto_metadata(&self.metadata),
            ..Default::default()
        };

        return proto_object;
    }
}
