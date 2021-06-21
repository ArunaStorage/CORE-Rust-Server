use std::time::Duration;

use async_trait::async_trait;

use log::error;
use rusoto_core::{
    credential::{DefaultCredentialsProvider, ProvideAwsCredentials},
    Region,
};
use rusoto_s3::{
    util::{PreSignedRequest, PreSignedRequestOption},
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CreateMultipartUploadRequest,
    DeleteObjectRequest, GetObjectRequest, PutObjectRequest, S3Client, UploadPartRequest, S3,
};
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::CompletedParts;

use super::objectstorage::StorageHandler;
use crate::models::{
    common_models::{IndexLocation, Location, LocationType},
    dataset_object_group::DatasetObject,
};

use crate::SETTINGS;

/// Handles S3-compatible object storage backends for storing data
/// Access is entirely provided via presigned URLs
/// For large upload (>3GB) it is necessary to use multipart uploads, they are provided via
/// presigned urls as well. The stored object metadata has an upload_id field that stores
/// the associated upload_id. Part number and etag of each individual upload have to be provided during finish upload
/// TODO: Update object status after finished upload.
/// TODO: Check if object has already been uploaded, an upload should not never occur with the same key twice to avoid consistency problems
pub struct S3Handler {
    client: S3Client,
    bucket: String,
    endpoint: String,
    region: Region,
    credentials: DefaultCredentialsProvider,
}

impl S3Handler {
    pub fn new() -> Self {
        let endpoint = SETTINGS
            .read()
            .unwrap()
            .get_str("Storage.Endpoint")
            .unwrap_or("localhost".to_string());
        let bucket = SETTINGS.read().unwrap().get_str("Storage.Bucket").unwrap();
        let region = "RegionOne".to_string();

        let creds = DefaultCredentialsProvider::new().unwrap();

        let region = Region::Custom {
            name: region.clone(),
            endpoint: endpoint.clone(),
        };

        let s3_handler = S3Handler {
            client: S3Client::new(region.clone()),
            bucket: bucket,
            endpoint: endpoint,
            region: region,
            credentials: creds,
        };

        return s3_handler;
    }
}

#[async_trait]
impl StorageHandler for S3Handler {
    async fn create_location(
        &self,
        project_id: String,
        dataset_id: String,
        object_id: String,
        filename: String,
        _index: Option<crate::models::common_models::IndexLocation>,
    ) -> Result<crate::models::common_models::Location, tonic::Status> {
        let object_key = format!("{}/{}/{}/{}", project_id, dataset_id, object_id, filename);
        let location = Location {
            bucket: self.bucket.clone(),
            key: object_key,
            url: self.endpoint.clone(),
            location_type: LocationType::Object,
            index_location: IndexLocation {
                start_byte: 0,
                end_byte: 0,
            },
        };

        Ok(location)
    }

    async fn create_download_link(
        &self,
        location: crate::models::common_models::Location,
    ) -> Result<String, tonic::Status> {
        let object_request = GetObjectRequest {
            bucket: location.bucket,
            key: location.key,
            ..Default::default()
        };

        let presign_options = PreSignedRequestOption {
            expires_in: Duration::from_secs(3600),
        };

        let credentials = match self.credentials.credentials().await {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal("error when creating download link"));
            }
        };

        let url = object_request.get_presigned_url(&self.region, &credentials, &presign_options);
        Ok(url)
    }

    async fn create_upload_link(
        &self,
        location: crate::models::common_models::Location,
    ) -> Result<String, tonic::Status> {
        let object_request = PutObjectRequest {
            bucket: location.bucket,
            key: location.key,
            ..Default::default()
        };

        let presign_options = PreSignedRequestOption {
            expires_in: Duration::from_secs(3600),
        };

        let credentials = match self.credentials.credentials().await {
            Ok(value) => value,
            Err(e) => {
                error!("{:?}", e);
                return Err(tonic::Status::internal("error when creating upload link"));
            }
        };

        let url = object_request.get_presigned_url(&self.region, &credentials, &presign_options);
        Ok(url)
    }

    async fn init_multipart_upload(
        &self,
        object: &DatasetObject,
    ) -> std::result::Result<String, tonic::Status> {
        let multipart_create_req = CreateMultipartUploadRequest {
            bucket: self.get_bucket(),
            key: object.location.key.clone(),
            ..Default::default()
        };

        let create_resp = match self
            .client
            .create_multipart_upload(multipart_create_req)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e.to_string());
                return Err(tonic::Status::internal("error initiating multipart upload"));
            }
        };

        let object_id = object.id.clone();

        let upload_id = match create_resp.upload_id {
            Some(value) => value,
            None => {
                log::error!(
                    "could not create multipart upload for object with id: {}",
                    object_id
                );
                return Err(tonic::Status::internal("error initiating multipart upload"));
            }
        };

        return Ok(upload_id);
    }

    fn get_bucket(&self) -> String {
        return self.bucket.clone();
    }

    async fn upload_multipart_part_link(
        &self,
        location: &Location,
        upload_id: &str,
        upload_part: i64,
    ) -> std::result::Result<String, tonic::Status> {
        let presign_options = PreSignedRequestOption {
            expires_in: Duration::from_secs(3600),
        };

        let credentials = match self.credentials.credentials().await {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e.to_string());
                return Err(tonic::Status::internal(
                    "error creating object storage credentials",
                ));
            }
        };

        let upload_request = UploadPartRequest {
            bucket: location.bucket.clone(),
            key: location.key.clone(),
            upload_id: upload_id.to_string(),
            part_number: upload_part,
            ..Default::default()
        };

        let upload_link =
            upload_request.get_presigned_url(&self.region, &credentials, &presign_options);

        return Ok(upload_link);
    }

    async fn finish_multipart_upload(
        &self,
        location: &Location,
        objects: &Vec<CompletedParts>,
        upload_id: &str,
    ) -> Result<(), tonic::Status> {
        let mut upload_objects = Vec::new();

        for uploaded_object in objects {
            let completed_part_s3 = rusoto_s3::CompletedPart {
                e_tag: Some(uploaded_object.etag.clone()),
                part_number: Some(uploaded_object.part),
            };

            upload_objects.push(completed_part_s3);
        }

        let completed = CompletedMultipartUpload {
            parts: Some(upload_objects),
        };

        let completion_request = CompleteMultipartUploadRequest {
            bucket: location.bucket.clone(),
            key: location.key.clone(),
            upload_id: upload_id.to_string(),
            multipart_upload: Some(completed),
            ..Default::default()
        };

        let _completed_reponse = match self
            .client
            .complete_multipart_upload(completion_request)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                log::error!("{:?}", e.to_string());
                return Err(tonic::Status::internal("error completing multipart upload"));
            }
        };

        return Ok(());
    }

    async fn delete_object(&self, location: Location) -> std::result::Result<(), tonic::Status> {
        match self
            .client
            .delete_object(DeleteObjectRequest {
                bucket: location.bucket.clone(),
                key: location.key.clone(),
                ..Default::default()
            })
            .await
        {
            Ok(_) => (),
            Err(e) => {
                log::error!("{:?}", e.to_string());
                return Err(tonic::Status::internal("error deleting object"));
            }
        }

        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use std::{env, iter::FromIterator, path::PathBuf, sync::Once};

    use config::File;
    use scienceobjectsdb_rust_api::sciobjectsdbapi::services::{self, CreateObjectRequest};

    use crate::{
        models::dataset_object_group::DatasetObject, objectstorage::objectstorage::StorageHandler,
    };

    use super::S3Handler;

    use crate::SETTINGS;

    static INIT: Once = Once::new();

    #[tokio::test]
    async fn test_s3_download() {
        INIT.call_once(|| {
            match env::var("MONGO_PASSWORD") {
                Ok(_) => {}
                Err(_) => env::set_var("MONGO_PASSWORD", "test123"),
            }

            match env::var("AWS_ACCESS_KEY_ID") {
                Ok(_) => {}
                Err(_) => env::set_var("AWS_ACCESS_KEY_ID", "minioadmin"),
            }

            match env::var("AWS_SECRET_ACCESS_KEY") {
                Ok(_) => {}
                Err(_) => env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            }

            let mut testpath = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            testpath.push("resources/test/config.yaml");

            let conf_path = testpath.to_str().unwrap();
            SETTINGS
                .write()
                .unwrap()
                .merge(File::with_name(conf_path))
                .unwrap();
        });

        let uuid = uuid::Uuid::new_v4();

        let test_data = "testdata".to_string();

        let s3_handler = S3Handler::new();
        let location = s3_handler
            .create_location(
                "testproject".to_string(),
                uuid.to_string(),
                uuid.to_string(),
                "bytes".to_string(),
                None,
            )
            .await
            .unwrap();

        let upload_link = s3_handler.create_upload_link(location).await.unwrap();

        let client = reqwest::Client::new();
        let resp = client
            .put(upload_link)
            .body(test_data.clone())
            .send()
            .await
            .unwrap();
        if resp.status() != 200 {
            let status = resp.status();
            let msg = resp.text().await.unwrap();
            dbg!("{} - {}", status, msg);
            panic!("wrong status code when uploading to S3")
        }

        let location = s3_handler
            .create_location(
                "testproject".to_string(),
                uuid.to_string(),
                uuid.to_string(),
                "bytes".to_string(),
                None,
            )
            .await
            .unwrap();
        let download_link = s3_handler.create_download_link(location).await.unwrap();

        let resp = client.get(download_link).send().await.unwrap();

        if resp.status() != 200 {
            let status = resp.status();
            let msg = resp.text().await.unwrap();
            dbg!("{} - {}", status, msg);
            panic!("wrong status code when downloading from S3")
        }

        let data = resp.bytes().await.unwrap();
        let data_string = String::from_utf8(data.to_vec()).unwrap();

        if data_string != test_data {
            panic!("downloaded data does not match uploaded rata")
        }
    }

    #[tokio::test]
    async fn test_s3_multipart() {
        INIT.call_once(|| {
            match env::var("MONGO_PASSWORD") {
                Ok(_) => {}
                Err(_) => env::set_var("MONGO_PASSWORD", "test123"),
            }

            match env::var("AWS_ACCESS_KEY_ID") {
                Ok(_) => {}
                Err(_) => env::set_var("AWS_ACCESS_KEY_ID", "minioadmin"),
            }

            match env::var("AWS_SECRET_ACCESS_KEY") {
                Ok(_) => {}
                Err(_) => env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            }

            let mut testpath = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            testpath.push("resources/test/config.yaml");

            let conf_path = testpath.to_str().unwrap();
            SETTINGS
                .write()
                .unwrap()
                .merge(File::with_name(conf_path))
                .unwrap();
        });

        let uuid = uuid::Uuid::new_v4();

        let s3_bucket = SETTINGS.read().unwrap().get_str("Storage.Bucket").unwrap();

        let s3_handler = S3Handler::new();

        let create_object_req = CreateObjectRequest {
            filename: "testfile".to_string(),
            labels: Vec::new(),
            filetype: "binary".to_string(),
            metadata: Vec::new(),
            ..Default::default()
        };

        let mut data_1_vec = Vec::new();
        for _x in 0..10000000 {
            data_1_vec.push("ABC");
        }

        let data_1 = String::from_iter(data_1_vec);

        let mut data_2_vec = Vec::new();
        for _x in 0..5000 {
            data_2_vec.push("ABC");
        }

        let data_2 = String::from_iter(data_2_vec);

        let mut test_data: String = "".to_string();
        test_data.push_str(&data_1);
        test_data.push_str(&data_2);

        let object = DatasetObject::new_from_proto_create(
            &create_object_req,
            uuid.to_string(),
            s3_bucket.clone(),
        )
        .unwrap();

        let upload_id = s3_handler.init_multipart_upload(&object).await.unwrap();

        let upload_link_1 = s3_handler
            .upload_multipart_part_link(&object.location, upload_id.as_str(), 1)
            .await
            .unwrap();
        let upload_link_2 = s3_handler
            .upload_multipart_part_link(&object.location, upload_id.as_str(), 2)
            .await
            .unwrap();

        let mut uploaded = Vec::new();

        let client = reqwest::Client::new();
        let resp = client.put(upload_link_1).body(data_1).send().await.unwrap();
        if resp.status() != 200 {
            let status = resp.status();
            let msg = resp.text().await.unwrap();
            dbg!("{} - {}", status, msg);
            panic!("wrong status code when uploading to S3")
        }

        let etag_1 = match resp.headers().get("etag") {
            Some(value) => value,
            None => {
                panic!("could not extract etag from header")
            }
        };

        let uploaded_part_1 = services::CompletedParts {
            etag: etag_1.to_str().unwrap().to_string(),
            part: 1,
        };

        uploaded.push(uploaded_part_1);

        let resp = client.put(upload_link_2).body(data_2).send().await.unwrap();
        if resp.status() != 200 {
            let status = resp.status();
            let msg = resp.text().await.unwrap();
            dbg!("{} - {}", status, msg);
            panic!("wrong status code when uploading to S3")
        }

        let etag_2 = match resp.headers().get("etag") {
            Some(value) => value,
            None => {
                panic!("could not extract etag from header")
            }
        };

        let uploaded_part_2 = services::CompletedParts {
            etag: etag_2.to_str().unwrap().to_string(),
            part: 2,
        };

        uploaded.push(uploaded_part_2);

        s3_handler
            .finish_multipart_upload(&object.location, &uploaded, upload_id.as_str())
            .await
            .unwrap();

        let download_link = s3_handler
            .create_download_link(object.location.clone())
            .await
            .unwrap();

        let resp = client.get(download_link).send().await.unwrap();

        if resp.status() != 200 {
            let status = resp.status();
            let msg = resp.text().await.unwrap();
            dbg!("{} - {}", status, msg);
            panic!("wrong status code when downloading from S3")
        }

        let data = resp.bytes().await.unwrap();
        let data_string = String::from_utf8(data.to_vec()).unwrap();

        if data_string != test_data {
            panic!("downloaded data does not match uploaded rata")
        }
    }
}
