use std::time::SystemTime;

use async_trait::async_trait;
use mongodb::bson::{doc, from_document, to_document, DateTime, Document};
use serde::{Deserialize, Serialize};

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;

use super::{dataset_model::DatasetEntry, dataset_object_group::ObjectGroup};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ResultWrapperSync<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct User {
    pub user_id: String,
    pub rights: Vec<Right>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum Resource {
    Project,
    Dataset,
    DatasetVersion,
    ObjectGroup,
    ObjectGroupVersion,
    Object,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Right {
    Read,
    Write,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Label {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Metadata {
    pub key: String,
    pub labels: Vec<Label>,
    pub metadata: Vec<u8>,
    pub schema: Option<Schema>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Schema {
    SimpleSchema(String),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Available,
    Initializing,
    Updating,
    Archived,
    Deleting,
}

#[allow(dead_code)]
pub fn to_status(proto_status: &models::Status) -> Status {
    match proto_status {
        models::Status::Initiating => return Status::Initializing,
        models::Status::Available => return Status::Available,
        models::Status::Updating => return Status::Updating,
        models::Status::Archived => return Status::Archived,
        models::Status::Deleting => return Status::Deleting,
    }
}

pub fn to_proto_status(status: &Status) -> models::Status {
    match status {
        Status::Available => return models::Status::Available,
        Status::Initializing => return models::Status::Initiating,
        Status::Updating => return models::Status::Updating,
        Status::Archived => return models::Status::Archived,
        Status::Deleting => return models::Status::Deleting,
    }
}

impl Default for Status {
    fn default() -> Self {
        Status::Available
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct Version {
    major: i32,
    minor: i32,
    patch: i32,
    revision: i32,
    version_stage: VersionStage,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum VersionStage {
    Stable,
    ReleaseCandidate,
    Beta,
    Alpha,
}

impl Default for VersionStage {
    fn default() -> Self {
        VersionStage::Alpha
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    link: String,
    location: Location,
    origin_type: OriginType,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub bucket: String,
    pub key: String,
    pub url: String,
    pub location_type: LocationType,
    pub index_location: IndexLocation,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum LocationType {
    Object,
    Index,
}

impl Default for LocationType {
    fn default() -> Self {
        LocationType::Object
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct IndexLocation {
    pub start_byte: i64,
    pub end_byte: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum OriginType {
    ObjectStorage,
    ObjectLink,
}

impl Default for OriginType {
    fn default() -> Self {
        OriginType::ObjectStorage
    }
}

pub trait DatabaseModel<'de>: serde::Serialize + serde::de::DeserializeOwned + Send + Sync {
    fn to_document(&self) -> ResultWrapperSync<Document> {
        let document = to_document(self)?;

        Ok(document)
    }

    fn new_from_document(document: Document) -> ResultWrapper<Self> {
        let model: Self = from_document(document)?;
        Ok(model)
    }

    fn get_model_name() -> ResultWrapper<String>;
}

#[async_trait]
pub trait DatabaseHandler<'de, T: DatabaseModel<'de>> {
    async fn store_model_entry(&self, value: T) -> ResultWrapper<T>;
    async fn get_model_entry(&self, id: String) -> ResultWrapper<Option<T>>;
    async fn find(&self, id: String, key: String) -> ResultWrapper<Vec<T>>;
    async fn find_one(&self, id: String, key: String) -> ResultWrapper<Option<T>>;
}

#[async_trait]
pub trait DatasetHandlerTrait {
    async fn get_project_datasets(&self, project_id: String) -> ResultWrapper<Vec<DatasetEntry>>;
}

#[async_trait]
pub trait ObjectGroupHandlerTrait {
    async fn get_dataset_object_group(&self, dataset_id: String)
        -> ResultWrapper<Vec<ObjectGroup>>;
}

pub fn to_metadata(proto_metadata: &Vec<models::Metadata>) -> Vec<Metadata> {
    let mut metadata = Vec::new();

    for proto_metadata_entry in proto_metadata {
        let metadata_entry = Metadata {
            key: proto_metadata_entry.key.clone(),
            metadata: proto_metadata_entry.metadata.clone(),
            labels: to_labels(&proto_metadata_entry.labels),
            ..Default::default()
        };

        metadata.push(metadata_entry);
    }

    metadata
}

pub fn to_labels(proto_labels: &Vec<models::Label>) -> Vec<Label> {
    let mut labels = Vec::new();

    for proto_label in proto_labels {
        let label = Label {
            key: proto_label.key.clone(),
            value: proto_label.value.clone(),
        };

        labels.push(label);
    }

    return labels;
}

#[allow(dead_code)]
pub fn to_users(proto_users: &Vec<models::User>) -> Vec<User> {
    let mut users = Vec::new();

    for proto_user in proto_users {
        let user = User {
            user_id: proto_user.user_id.clone(),
            rights: to_rights(proto_user.rights.clone()),
        };

        users.push(user);
    }

    return users;
}

pub fn to_rights(proto_rights: Vec<i32>) -> Vec<Right> {
    let mut rights = Vec::new();

    for proto_right_id in proto_rights {
        let proto_right: models::Right = models::Right::from_i32(proto_right_id).unwrap();
        let right = match proto_right {
            models::Right::Read => Right::Read,
            models::Right::Write => Right::Write,
        };

        rights.push(right);
    }

    return rights;
}

pub fn to_version(version: models::Version) -> Version {
    let version = Version {
        major: version.major,
        minor: version.minor,
        patch: version.patch,
        revision: version.revision,
        version_stage: VersionStage::Stable,
    };

    return version;
}

pub fn to_proto_metadata(metadata: &Vec<Metadata>) -> Vec<models::Metadata> {
    let mut proto_metadata = Vec::new();
    for metadata_entry in metadata {
        let proto_metadata_entry = models::Metadata {
            key: metadata_entry.key.to_string(),
            labels: to_proto_labels(&metadata_entry.labels),
            metadata: metadata_entry.metadata.clone(),
            ..Default::default()
        };

        proto_metadata.push(proto_metadata_entry)
    }

    proto_metadata
}

pub fn to_proto_labels(labels: &Vec<Label>) -> Vec<models::Label> {
    let mut proto_labels = Vec::new();
    for label in labels {
        let proto_label = models::Label {
            key: label.key.to_string(),
            value: label.value.to_string(),
        };

        proto_labels.push(proto_label)
    }

    proto_labels
}

pub fn to_proto_users(users: &Vec<User>) -> Vec<models::User> {
    let mut proto_users = Vec::new();

    for user in users {
        let proto_user = models::User {
            user_id: user.user_id.to_string(),
            rights: to_proto_rights(&user.rights),
            ..Default::default()
        };

        proto_users.push(proto_user);
    }

    proto_users
}

pub fn to_proto_rights(rights: &Vec<Right>) -> Vec<i32> {
    let mut proto_rights = Vec::new();

    for right in rights {
        let proto_right = match right {
            Right::Write => models::Right::Write as i32,
            Right::Read => models::Right::Read as i32,
        };

        proto_rights.push(proto_right);
    }

    return proto_rights;
}

pub fn to_proto_version(version: &Version) -> models::Version {
    let version = models::Version {
        major: version.major,
        minor: version.minor,
        patch: version.patch,
        revision: version.revision,
        ..Default::default()
    };

    return version;
}

pub fn to_proto_datetime(datetime: &DateTime) -> prost_types::Timestamp {
    let system_date_time = SystemTime::from(datetime.0);
    return prost_types::Timestamp::from(system_date_time);
}
