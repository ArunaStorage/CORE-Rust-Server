use bson::doc;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::CompletedParts;

use crate::{
    database::database::Database,
    models::dataset_object_group::{DatasetObject, ObjectGroup, ObjectGroupRevision},
};

use super::common::CommonHandler;

/// Handles data load operations
/// The data is stored in an object storage and access is negotiated via presigned URLs
/// Uploads to a single link are limited in size by the underlaying object storage. In general it is recommended to
/// use the multipart upload for object larger than 15MB
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

    /// Initiates a multipart upload. It returns the object which is associated with the uploaded object
    /// If a multipart upload is initiated the upload_id field is set
    /// This upload_id can be used to generate individual upload links with the create_multipart_upload_link
    /// The underlaying object storage implementation usually sets limits for the minimum required part size
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

    /// Creates a multipart upload link
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

    /// Finishes a multipart upload
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

    /// Marks an object group as available
    /// This is required to allow the user to indicate a finished upload
    /// The system itself is not able to determine if all objects of an object group are already uploaded
    pub async fn finish_object_group_upload(&self, id: &str) -> Result<(), tonic::Status> {
        self.update_status::<ObjectGroup>(id, &crate::models::common_models::Status::Available)
            .await?;

        Ok(())
    }
}
