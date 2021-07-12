use mongodb::bson::{doc, from_document, to_document, Document};
use serde::{Deserialize, Serialize};

use log::error;

use scienceobjectsdb_rust_api::sciobjectsdbapi::models;

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
    ObjectGroupRevision,
    Object,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
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
pub fn to_status(proto_status: &models::v1::Status) -> Status {
    match proto_status {
        models::v1::Status::Initiating => return Status::Initializing,
        models::v1::Status::Available => return Status::Available,
        models::v1::Status::Updating => return Status::Updating,
        models::v1::Status::Archived => return Status::Archived,
        models::v1::Status::Deleting => return Status::Deleting,
    }
}

pub fn to_proto_status(status: &Status) -> models::v1::Status {
    match status {
        Status::Available => return models::v1::Status::Available,
        Status::Initializing => return models::v1::Status::Initiating,
        Status::Updating => return models::v1::Status::Updating,
        Status::Archived => return models::v1::Status::Archived,
        Status::Deleting => return models::v1::Status::Deleting,
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

    fn new_from_document(document: Document) -> Result<Self, tonic::Status> {
        let model: Self = match from_document(document) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal(format!(
                    "error when parsing documents"
                )));
            }
        };
        Ok(model)
    }

    fn get_model_name() -> Result<String, tonic::Status>;

    fn get_parent_field_name() -> Result<String, tonic::Status>;
}

pub fn to_metadata(proto_metadata: &Vec<models::v1::Metadata>) -> Vec<Metadata> {
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

pub fn to_labels(proto_labels: &Vec<models::v1::Label>) -> Vec<Label> {
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
pub fn to_users(proto_users: &Vec<models::v1::User>) -> Vec<User> {
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
        let proto_right: models::v1::Right = models::v1::Right::from_i32(proto_right_id).unwrap();
        let right = match proto_right {
            models::v1::Right::Read => Right::Read,
            models::v1::Right::Write => Right::Write,
        };

        rights.push(right);
    }

    return rights;
}

pub fn to_version(version: models::v1::Version) -> Version {
    let version = Version {
        major: version.major,
        minor: version.minor,
        patch: version.patch,
        revision: version.revision,
        version_stage: VersionStage::Stable,
    };

    return version;
}

pub fn to_proto_metadata(metadata: &Vec<Metadata>) -> Vec<models::v1::Metadata> {
    let mut proto_metadata = Vec::new();
    for metadata_entry in metadata {
        let proto_metadata_entry = models::v1::Metadata {
            key: metadata_entry.key.to_string(),
            labels: to_proto_labels(&metadata_entry.labels),
            metadata: metadata_entry.metadata.clone(),
            ..Default::default()
        };

        proto_metadata.push(proto_metadata_entry)
    }

    proto_metadata
}

pub fn to_proto_labels(labels: &Vec<Label>) -> Vec<models::v1::Label> {
    let mut proto_labels = Vec::new();
    for label in labels {
        let proto_label = models::v1::Label {
            key: label.key.to_string(),
            value: label.value.to_string(),
        };

        proto_labels.push(proto_label)
    }

    proto_labels
}

pub fn to_proto_users(users: &Vec<User>) -> Vec<models::v1::User> {
    let mut proto_users = Vec::new();

    for user in users {
        let proto_user = models::v1::User {
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
            Right::Write => models::v1::Right::Write as i32,
            Right::Read => models::v1::Right::Read as i32,
        };

        proto_rights.push(proto_right);
    }

    return proto_rights;
}

pub fn to_proto_version(version: &Version) -> models::v1::Version {
    let version = models::v1::Version {
        major: version.major,
        minor: version.minor,
        patch: version.patch,
        revision: version.revision,
        ..Default::default()
    };

    return version;
}
