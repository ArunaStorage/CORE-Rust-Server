use std::{error::Error, fmt};

use reqwest::Client;
use serde_json::Value;

use crate::SETTINGS;

pub struct OAuth2Handler {
    user_info_endpoint_url: String,
    client: Client,
}

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

impl OAuth2Handler {
    pub fn new() -> ResultWrapper<Self> {
        let client = Client::new();

        let endpoint_url = SETTINGS
            .read()
            .unwrap()
            .get_str("Oauth2Auth.UserInfoEndpoint")?;

        Ok(OAuth2Handler {
            user_info_endpoint_url: endpoint_url,
            client: client,
        })
    }

    pub async fn parse_user_id_from_token(&self, token: String) -> ResultWrapper<String> {
        let response = self
            .client
            .get(self.user_info_endpoint_url.clone())
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()?;

        let data = response.text().await?;

        let parsed_struct: Value = serde_json::from_str(&data)?;
        let user_id = parsed_struct.get("sub").unwrap().to_string();

        Ok(user_id)
    }
}
