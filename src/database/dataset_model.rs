use scienceobjectsdb_rust_api::sciobjectsdbapi::{models, services};
use serde::{Deserialize, Serialize};

use chrono::prelude::*;
use mongodb::bson::DateTime;

use super::common_models::{
    to_labels, to_metadata, to_proto_labels, to_proto_metadata, to_proto_status, DatabaseModel,
    Label, Metadata, Status,
};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DatasetEntry {
    pub id: String,
    pub name: String,
    pub description: String,
    pub is_public: bool,
    pub created: DateTime,
    pub status: Status,
    pub project_id: String,
    pub labels: Vec<Label>,
    pub metadata: Vec<Metadata>,
}

impl DatabaseModel<'_> for DatasetEntry {
    fn get_model_name() -> Result<String, tonic::Status> {
        Ok("Dataset".to_string())
    }
}

impl DatasetEntry {
    pub fn new_from_proto_create(
        request: &services::CreateDatasetRequest,
    ) -> Result<Self, tonic::Status> {
        let uuid = uuid::Uuid::new_v4();
        let timestamp = Utc::now();

        let dataset_entry = DatasetEntry {
            id: uuid.to_string(),
            name: request.name.clone(),
            created: DateTime::from(timestamp),
            is_public: false,
            labels: to_labels(&request.labels),
            project_id: request.project_id.clone(),
            metadata: to_metadata(&request.metadata),
            status: Status::Available,
            description: "".to_string(),
        };

        Ok(dataset_entry)
    }

    pub fn to_proto_dataset(&self) -> models::Dataset {
        let dataset = models::Dataset {
            id: self.id.to_string(),
            name: self.name.to_string(),
            created: None,
            description: self.description.to_string(),
            is_public: self.is_public,
            labels: to_proto_labels(&self.labels),
            metadata: to_proto_metadata(&self.metadata),
            project_id: self.project_id.to_string(),
            status: to_proto_status(&self.status) as i32,
            ..Default::default()
        };

        return dataset;
    }
}
