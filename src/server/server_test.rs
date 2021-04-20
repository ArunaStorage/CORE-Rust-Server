use config::File;
use core::time;
use std::{env, path::PathBuf, sync::Once, thread};

use crate::SETTINGS;

static INIT: Once = Once::new();

pub fn test_init() {
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

    let mut is_completed = INIT.is_completed();
    let wait_for_completion_wait = time::Duration::from_millis(500);
    while !is_completed {
        dbg!("foo");
        thread::sleep(wait_for_completion_wait);
        is_completed = INIT.is_completed();
    }
}

#[cfg(test)]
mod server_test {
    use std::sync::Arc;

    use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_objects_service_server::DatasetObjectsService;
    use scienceobjectsdb_rust_api::sciobjectsdbapi::services::dataset_service_server::DatasetService;
    use scienceobjectsdb_rust_api::sciobjectsdbapi::services::object_load_server::ObjectLoad;
    use scienceobjectsdb_rust_api::sciobjectsdbapi::services::project_api_server::ProjectApi;

    use scienceobjectsdb_rust_api::sciobjectsdbapi::models;
    use scienceobjectsdb_rust_api::sciobjectsdbapi::services;

    use tonic::Request;

    use super::test_init;

    use super::SETTINGS;

    use crate::database::mongo_connector::MongoHandler;
    use crate::objectstorage::s3_objectstorage::S3Handler;
    use crate::server::{
        dataset_api::DatasetsServer, load_api::LoadServer, object_api::ObjectServer,
        project_api::ProjectServer,
    };

    use crate::auth::test_authenticator::TestAuthenticator;

    #[tokio::test]
    async fn full_test() {
        test_init();

        let s3_endpoint = SETTINGS
            .read()
            .unwrap()
            .get_str("Storage.Endpoint")
            .unwrap_or("localhost".to_string());
        let s3_bucket = SETTINGS.read().unwrap().get_str("Storage.Bucket").unwrap();
        let _database_name = SETTINGS
            .read()
            .unwrap()
            .get_str("Database.Mongo.Database")
            .unwrap_or("test-database".to_string());

        let mongo_handler = Arc::new(MongoHandler::new(s3_bucket.clone()).await.unwrap());

        let object_storage_handler = Arc::new(S3Handler::new(
            s3_endpoint.to_string(),
            "RegionOne".to_string(),
            s3_bucket.clone(),
        ));

        let authz_handler = Arc::new(TestAuthenticator {});

        let project_endpoints = ProjectServer {
            mongo_client: mongo_handler.clone(),
            auth_handler: authz_handler.clone(),
        };

        let dataset_endpoints = DatasetsServer {
            mongo_client: mongo_handler.clone(),
            auth_handler: authz_handler.clone(),
        };

        let objects_endpoints = ObjectServer {
            mongo_client: mongo_handler.clone(),
            object_handler: object_storage_handler.clone(),
            auth_handler: authz_handler.clone(),
        };

        let load_endpoints = LoadServer {
            mongo_client: mongo_handler.clone(),
            object_handler: object_storage_handler.clone(),
            auth_handler: authz_handler.clone(),
        };

        let create_project_request = Request::new(services::CreateProjectRequest {
            name: "testproject".to_string(),
            description: "Some description".to_string(),
            ..Default::default()
        });

        let create_project_request_2 = Request::new(services::CreateProjectRequest {
            name: "testproject".to_string(),
            description: "Some description".to_string(),
            ..Default::default()
        });

        let project = project_endpoints
            .create_project(create_project_request)
            .await
            .unwrap()
            .into_inner();

        let _project_2 = project_endpoints
            .create_project(create_project_request_2)
            .await
            .unwrap()
            .into_inner();

        let user_projects = project_endpoints
            .get_user_projects(Request::new(models::Empty::default()))
            .await
            .unwrap()
            .into_inner();
        if user_projects.projects.len() != 2 {
            panic!("wrong number of projects found for user in test")
        }

        let create_dataset_request = Request::new(services::CreateDatasetRequest {
            project_id: project.id,
            name: "testdataset".to_string(),
            ..Default::default()
        });

        let dataset = dataset_endpoints
            .create_new_dataset(create_dataset_request)
            .await
            .unwrap()
            .into_inner();

        let create_object_request = services::CreateObjectRequest {
            filename: "testobject.txt".to_string(),
            filetype: "txt".to_string(),
            content_len: 8,
            ..Default::default()
        };

        let create_object_group_request = Request::new(services::CreateObjectGroupRequest {
            dataset_id: dataset.id,
            name: "testobjectgroup".to_string(),
            objects: vec![create_object_request],
            ..Default::default()
        });

        let object_group = objects_endpoints
            .create_object_group(create_object_group_request)
            .await
            .unwrap()
            .into_inner();

        let object_id = object_group.objects[0].id.clone();

        let upload_request = Request::new(models::Id {
            id: object_id.clone(),
        });

        let upload_link = load_endpoints
            .create_upload_link(upload_request)
            .await
            .unwrap()
            .into_inner();
        let test_data = "testdata";

        let client = reqwest::Client::new();
        let resp = client
            .put(upload_link.upload_link)
            .body(test_data.clone())
            .send()
            .await
            .unwrap();

        if resp.status() != 200 {
            if resp.status() != 200 {
                let status = resp.status();
                let msg = resp.text().await.unwrap();
                panic!(
                    "wrong status code when uploading to S3: {} - {}",
                    status, msg
                )
            }
        }

        let download_request = Request::new(models::Id {
            id: object_id.clone(),
        });

        let download_link = load_endpoints
            .create_download_link(download_request)
            .await
            .unwrap()
            .into_inner();

        let resp = client.get(download_link.upload_link).send().await.unwrap();
        if resp.status() != 200 {
            let status = resp.status();
            let msg = resp.text().await.unwrap();
            panic!(
                "wrong status code when downloading from S3: {} - {}",
                status, msg
            )
        }

        let data = resp.bytes().await.unwrap();
        let data_string = String::from_utf8(data.to_vec()).unwrap();

        if data_string != test_data {
            panic!("downloaded data does not match uploaded rata")
        }
    }
}
