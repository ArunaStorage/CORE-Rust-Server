use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};

use super::common_models::{
    DatabaseModel, Label, Location, Metadata, Origin, Status, User, Version,
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
    fn get_model_name() -> ResultWrapper<String> {
        Ok("Dataaset".to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectEntry {
    pub id: String,
    pub description: String,
    pub users: Vec<User>,
    pub name: String,
    pub labels: Vec<Label>,
    pub metadata: Vec<Metadata>,
}

impl DatabaseModel<'_> for ProjectEntry {
    fn get_model_name() -> ResultWrapper<String> {
        Ok("Rpoject".to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DatasetObjectGroup {
    pub id: String,
    pub name: String,
    pub version: Version,
    pub objects_count: i64,
    pub objects: Vec<DatasetObject>,
    pub labels: Vec<Label>,
    pub object_heritage_id: String,
    pub dataset_id: String,
    pub status: Status,
    pub metadata: Vec<Metadata>,
}

impl DatabaseModel<'_> for DatasetObjectGroup {
    fn get_model_name() -> ResultWrapper<String> {
        Ok("ObjectGroup".to_string())
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
    fn get_model_name() -> ResultWrapper<String> {
        Ok("Object".to_string())
    }
}

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