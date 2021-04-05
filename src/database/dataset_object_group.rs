use std::sync::Arc;

use chrono::{Timelike, Utc};
use mongodb::bson::DateTime;
use scienceobjectsdb_rust_api::sciobjectsdbapi::{models, services};
use serde::{Deserialize, Serialize};

use super::{
    common_models::{
        to_labels, to_metadata, to_proto_labels, to_proto_metadata, DatabaseModel, Label, Location,
        Metadata, Origin, Status, Version,
    },
    data_models::{DatasetEntry, DatasetObject, DatasetObjectGroup},
    mongo_connector::MongoHandler,
};

use super::common_models;

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error>>;

impl DatasetObjectGroup {
    pub fn new_from_proto_create(
        request: services::CreateObjectGroupRequest,
        bucket: String,
        handler: Arc<MongoHandler>,
    ) -> ResultWrapper<Self> {
        let uuid = uuid::Uuid::new_v4();

        let mut objects = Vec::new();
        let mut i = 0;
        for create_object in request.objects {
            i = i + 1;
            let object = DatasetObject::new_from_proto_create(
                create_object,
                request.dataset_id.clone(),
                bucket.clone(),
            )?;
            objects.push(object)
        }

        let object_group = DatasetObjectGroup {
            id: uuid.to_string(),
            name: request.name,
            objects_count: objects.len() as i64,
            objects: objects,
            labels: to_labels(request.labels),
            dataset_id: request.dataset_id,
            status: Status::Available,
            metadata: to_metadata(request.metadata),
            object_heritage_id: "".to_string(),
            version: Version::default(),
        };

        return Ok(object_group);
    }

    pub fn to_proto(&self) -> models::ObjectGroup {
        let mut objects = Vec::new();

        for object in &self.objects {
            let object_proto = object.to_proto_object();
            objects.push(object_proto)
        }

        let object_group = models::ObjectGroup {
            id: self.id.clone(),
            name: self.name.clone(),
            initialized_objects: self.objects_count,
            uploaded_objects: self.objects_count,
            objects: objects,
            labels: to_proto_labels(&self.labels),
            dataset_id: self.dataset_id.clone(),
            status: 0,
            metadata: to_proto_metadata(&self.metadata),
            ..Default::default()
        };

        return object_group;
    }
}

impl DatasetObject {
    pub fn new_from_proto_create(
        request: services::CreateObjectRequest,
        dataset_id: String,
        bucket: String,
    ) -> ResultWrapper<Self> {
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
            filename: request.filename,
            filetype: request.filetype,
            origin: Origin::default(),
            content_len: request.content_len,
            location: location,
            created: DateTime::from(timestamp),
            upload_id: "".to_string(),
            metadata: to_metadata(request.metadata),
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
