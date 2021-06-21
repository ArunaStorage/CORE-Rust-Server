use bson::{doc, to_document};
use log::error;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::AddUserToProjectRequest;

use crate::{
    database::database::Database,
    models::common_models::{DatabaseModel, Status},
};

use super::common::CommonHandler;

pub type UpdateHandler<T: Database> = CommonHandler<T>;

impl<T> UpdateHandler<T>
where
    T: Database,
{
    pub async fn update_status<'de, K: DatabaseModel<'de>>(
        &self,
        id: &str,
        status: &Status,
    ) -> Result<(), tonic::Status> {
        let query = doc! {
            "id": id
        };

        let enum_value = match to_document(status) {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal("error on status update"));
            }
        };

        let update = doc! {
            "$set": {
                "status": enum_value
            }
        };

        self.database_client
            .update_field::<K>(query, update)
            .await?;

        return Ok(());
    }

    pub async fn add_user_to_project(
        &self,
        add_user_request: &AddUserToProjectRequest,
    ) -> Result<(), tonic::Status> {
        return self.database_client.add_user(add_user_request).await;
    }
}
