use std::sync::Arc;

use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models,
    services::{self, project_api_client::ProjectApiClient},
};

use super::{common_models::*, data_models::ProjectEntry, mongo_connector::MongoHandler};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error>>;

impl ProjectEntry {
    pub fn new_from_proto_create(request: services::CreateProjectRequest) -> ResultWrapper<Self> {
        let uuid = uuid::Uuid::new_v4();
        let project = ProjectEntry {
            id: uuid.to_string(),
            name: request.name,
            description: request.description,
            metadata: to_metadata(request.metadata.to_vec()),
            ..Default::default()
        };

        return Ok(project);
    }

    pub fn to_proto_project(&self) -> models::Project {
        let proto_project = models::Project {
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
