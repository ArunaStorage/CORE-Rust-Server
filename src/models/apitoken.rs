use super::common_models::{to_proto_rights, DatabaseModel, Right};
use rand::Rng;
use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
use serde::{Deserialize, Serialize};

const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789)(*&^%$#@!~";
const TOKEN_LEN: usize = 30;

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct APIToken {
    pub id: String,
    pub user_id: String,
    pub token: String,
    pub rights: Vec<Right>,
    pub project_id: String,
}

impl DatabaseModel<'_> for APIToken {
    fn get_model_name() -> Result<String, tonic::Status> {
        Ok("APIToken".to_string())
    }

    fn get_parent_field_name() -> Result<String, tonic::Status> {
        Ok("parent_id".to_string())
    }
}

impl APIToken {
    pub fn new(user_id: &str, rights: Vec<Right>, project_id: &str) -> Result<Self, tonic::Status> {
        let uuid = uuid::Uuid::new_v4();
        let token = generate_api_token();

        let dataset_entry = APIToken {
            id: uuid.to_string(),
            user_id: user_id.to_string(),
            rights: rights,
            token: token,
            project_id: project_id.to_string(),
        };

        Ok(dataset_entry)
    }

    pub fn to_proto(&self) -> models::ApiToken {
        let api_token = models::ApiToken {
            id: self.id.clone(),
            rights: to_proto_rights(&self.rights),
            token: self.token.clone(),
            project_id: self.project_id.clone(),
        };

        return api_token;
    }
}

fn generate_api_token() -> String {
    let mut rng = rand::thread_rng();

    let token: String = (0..TOKEN_LEN)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    return token;
}
