use chrono::Utc;
use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};

use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models::{self},
    services,
};

use super::common_models::{
    to_labels, to_metadata, to_proto_datetime, to_proto_labels, to_proto_metadata, to_proto_status,
    to_proto_version, to_version, DatabaseModel, Label, Metadata, Status, Version,
};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DatasetVersion {
    pub id: String,
    pub dataset_id: String,
    pub description: String,
    pub labels: Vec<Label>,
    pub metadata: Vec<Metadata>,
    pub created: DateTime,
    pub version: Version,
    pub object_group_ids: Vec<String>,
    pub object_count: i64,
    pub status: Status,
}

impl DatabaseModel<'_> for DatasetVersion {
    fn get_model_name() -> ResultWrapper<String> {
        Ok("DatasetVersion".to_string())
    }
}

impl DatasetVersion {
    pub fn new_from_proto_create(
        request: services::ReleaseDatasetVersionRequest,
    ) -> ResultWrapper<Self> {
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
            object_group_ids: request.object_group_ids,
            status: super::common_models::Status::Available,
            version: to_version(request.version.unwrap()),
        };

        return Ok(dataset_version);
    }

    pub fn to_proto(&self) -> ResultWrapper<models::DatasetVersion> {
        let proto_version = models::DatasetVersion {
            id: self.id.clone(),
            dataset_id: self.dataset_id.clone(),
            description: self.description.clone(),
            labels: to_proto_labels(&self.labels),
            metadata: to_proto_metadata(&self.metadata),
            object_count: self.object_count,
            object_group_ids: self.object_group_ids.clone(),
            version: Some(to_proto_version(&self.version)),
            status: to_proto_status(&self.status) as i32,
            created: Some(to_proto_datetime(&self.created)),
        };

        return Ok(proto_version);
    }
}
