use std::time::Duration;

use async_trait::async_trait;

use rusoto_core::{
    credential::{DefaultCredentialsProvider, ProvideAwsCredentials},
    Client, Region,
};
use rusoto_s3::{
    util::{PreSignedRequest, PreSignedRequestOption},
    GetObjectRequest, PutObjectRequest, S3Client, S3,
};

use super::objectstorage::StorageHandler;
use crate::database::common_models::{IndexLocation, Location, LocationType};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ResultWrapperSync<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct S3Handler {
    client: S3Client,
    bucket: String,
    endpoint: String,
    region: Region,
    credentials: DefaultCredentialsProvider,
}

impl S3Handler {
    pub fn new(endpoint: String, region: String, bucket: String) -> Self {
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
        index: Option<crate::database::common_models::IndexLocation>,
    ) -> ResultWrapper<crate::database::common_models::Location> {
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
        location: crate::database::common_models::Location,
    ) -> ResultWrapper<String> {
        let object_request = GetObjectRequest {
            bucket: location.bucket,
            key: location.key,
            ..Default::default()
        };

        let presign_options = PreSignedRequestOption {
            expires_in: Duration::from_secs(3600),
        };

        let credentials = self.credentials.credentials().await?;

        let url = object_request.get_presigned_url(&self.region, &credentials, &presign_options);
        Ok(url)
    }

    async fn create_upload_link(
        &self,
        location: crate::database::common_models::Location,
    ) -> ResultWrapper<String> {
        let object_request = PutObjectRequest {
            bucket: location.bucket,
            key: location.key,
            ..Default::default()
        };

        let presign_options = PreSignedRequestOption {
            expires_in: Duration::from_secs(3600),
        };

        let credentials = self.credentials.credentials().await?;

        let url = object_request.get_presigned_url(&self.region, &credentials, &presign_options);
        Ok(url)
    }

    fn get_bucket(&self) -> String {
        return self.bucket.clone();
    }
}

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf, sync::Once};

    use config::File;

    use crate::objectstorage::objectstorage::StorageHandler;

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
        let s3_endpoint = SETTINGS
            .read()
            .unwrap()
            .get_str("Storage.Endpoint")
            .unwrap_or("localhost".to_string());
        let s3_bucket = SETTINGS.read().unwrap().get_str("Storage.Bucket").unwrap();

        let s3_handler = S3Handler::new(s3_endpoint, "RegionOne".to_string(), s3_bucket);
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
}
