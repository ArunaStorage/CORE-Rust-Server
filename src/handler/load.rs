use bson::doc;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::CompletedParts;

use crate::{
    database::database::Database,
    models::dataset_object_group::{DatasetObject, ObjectGroup, ObjectGroupRevision},
};

use super::common::CommonHandler;

pub type LoadHandler<T> = CommonHandler<T>;

impl<T> LoadHandler<T>
where
    T: Database,
{
    pub async fn create_upload_link(&self, id: &str) -> Result<String, tonic::Status> {
        let object = self.database_client.find_object(id).await?;
        let link = self
            .object_handler
            .create_upload_link(object.location)
            .await?;

        return Ok(link);
    }

    pub async fn create_download_link(&self, id: &str) -> Result<String, tonic::Status> {
        let object = self.database_client.find_object(id).await?;
        let link = self
            .object_handler
            .create_download_link(object.location)
            .await?;

        return Ok(link);
    }

    pub async fn init_multipart_upload(&self, id: &str) -> Result<DatasetObject, tonic::Status> {
        let object = self.database_client.find_object(id).await?;
        let upload_id = self.object_handler.init_multipart_upload(&object).await?;

        let upload_id_update_query = doc! {
            "objects.id": object.id.clone(),

        };

        let upload_id_update = doc! {
            "objects.$": upload_id,
        };

        self.database_client
            .update_field::<ObjectGroupRevision>(upload_id_update_query, upload_id_update)
            .await?;

        Ok(object.clone())
    }

    pub async fn create_multipart_upload_link(
        &self,
        id: &str,
        upload_part: i64,
    ) -> Result<String, tonic::Status> {
        let object = self.database_client.find_object(id).await?;
        let upload_url = self
            .object_handler
            .upload_multipart_part_link(&object.location, object.upload_id.as_str(), upload_part)
            .await?;

        Ok(upload_url)
    }

    pub async fn finish_multipart_upload(
        &self,
        id: &str,
        objects: &Vec<CompletedParts>,
    ) -> Result<(), tonic::Status> {
        let object = self.database_client.find_object(id).await?;
        self.object_handler
            .finish_multipart_upload(&object.location, objects, object.upload_id.as_str())
            .await?;

        Ok(())
    }

    pub async fn finish_object_group_upload(&self, id: &str) -> Result<(), tonic::Status> {
        self.update_status::<ObjectGroup>(id, &crate::models::common_models::Status::Available)
            .await?;

        Ok(())
    }
}
