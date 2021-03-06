use std::convert::TryFrom;
use std::time::SystemTime;

use chrono::DateTime;
use chrono::Utc;
use prost_types::Timestamp;
use serde::{Deserialize, Serialize};

use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models::{self},
    services,
};

use super::common_models::{
    to_labels, to_metadata, to_proto_labels, to_proto_metadata, to_proto_status, to_proto_version,
    to_version, DatabaseModel, Label, Metadata, Status, Version,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DatasetVersion {
    pub id: String,
    pub dataset_id: String,
    pub description: String,
    pub labels: Vec<Label>,
    pub metadata: Vec<Metadata>,
    pub created: DateTime<Utc>,
    pub version: Version,
    pub object_group_ids: Vec<String>,
    pub object_count: i64,
    pub status: Status,
}

impl DatabaseModel<'_> for DatasetVersion {
    fn get_model_name() -> Result<String, tonic::Status> {
        Ok("DatasetVersion".to_string())
    }

    fn get_parent_field_name() -> Result<String, tonic::Status> {
        Ok("dataset_id".to_string())
    }
}

impl DatasetVersion {
    pub fn new_from_proto_create(
        request: &services::v1::ReleaseDatasetVersionRequest,
    ) -> Result<Self, tonic::Status> {
        let uuid = uuid::Uuid::new_v4();
        let timestamp = Utc::now();

        let dataset_version = DatasetVersion {
            id: uuid.to_string(),
            dataset_id: request.dataset_id.clone(),
            description: "".to_string(),
            created: DateTime::from(timestamp),
            labels: to_labels(&request.labels),
            metadata: to_metadata(&request.metadata),
            object_count: request.object_group_ids.len() as i64,
            object_group_ids: request.object_group_ids.clone(),
            status: super::common_models::Status::Available,
            version: to_version(request.version.clone().unwrap()),
        };

        return Ok(dataset_version);
    }

    pub fn to_proto(&self) -> models::v1::DatasetVersion {
        let system_time: SystemTime = self.created.into();
        let timestamp = Timestamp::try_from(system_time).unwrap();

        let proto_version = models::v1::DatasetVersion {
            id: self.id.clone(),
            dataset_id: self.dataset_id.clone(),
            description: self.description.clone(),
            labels: to_proto_labels(&self.labels),
            metadata: to_proto_metadata(&self.metadata),
            object_count: self.object_count,
            object_group_ids: self.object_group_ids.clone(),
            version: Some(to_proto_version(&self.version)),
            status: to_proto_status(&self.status) as i32,
            created: Some(timestamp),
        };

        return proto_version;
    }
}
