use serde::{Deserialize, Serialize};
use std::vec;

use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models,
    services::{self},
};

use super::common_models::*;

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
    fn get_model_name() -> Result<String, tonic::Status> {
        Ok("project".to_string())
    }

    fn get_parent_field_name() -> Result<String, tonic::Status> {
        Err(tonic::Status::internal("project does not have a parent"))
    }
}

impl ProjectEntry {
    pub fn new_from_proto_create(
        request: &services::v1::CreateProjectRequest,
        user_id: String,
    ) -> Result<Self, tonic::Status> {
        let user = User {
            user_id: user_id,
            rights: vec![Right::Write, Right::Read],
        };

        let uuid = uuid::Uuid::new_v4();
        let project = ProjectEntry {
            id: uuid.to_string(),
            name: request.name.clone(),
            description: request.description.clone(),
            metadata: to_metadata(&request.metadata.to_vec()),
            users: vec![user],
            labels: to_labels(&request.labels),
        };

        return Ok(project);
    }

    pub fn to_proto_project(&self) -> models::v1::Project {
        let proto_project = models::v1::Project {
            id: self.id.to_string(),
            description: self.description.to_string(),
            labels: to_proto_labels(&self.labels),
            metadata: to_proto_metadata(&self.metadata),
            name: self.name.to_string(),
            users: to_proto_users(&self.users),
        };

        proto_project
    }
}
