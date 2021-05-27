use config::File;
use core::time;
use std::{env, path::PathBuf, sync::Once, thread};

use crate::SETTINGS;

#[allow(dead_code)]
static INIT: Once = Once::new();

#[allow(dead_code)]
pub fn test_init() {
    INIT.call_once(|| {
        env_logger::init();

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

    struct TestEndpointStruct {
        project_handler: ProjectServer<MongoHandler>,
        dataset_handler: DatasetsServer<MongoHandler>,
        object_handler: ObjectServer<MongoHandler>,
        load_handler: LoadServer<MongoHandler>,
    }

    const TEST_DATA_REV1: &'static str = "testdata-revision-1";
    const TEST_DATA_REV2: &'static str = "testdata-revision-2";

    async fn test_endpoint_structs() -> TestEndpointStruct {
        let s3_endpoint = SETTINGS
            .read()
            .unwrap()
            .get_str("Storage.Endpoint")
            .unwrap_or("localhost".to_string());
        let s3_bucket = SETTINGS.read().unwrap().get_str("Storage.Bucket").unwrap();

        let mongo_handler = Arc::new(MongoHandler::new().await.unwrap());

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
            database_client: mongo_handler.clone(),
            auth_handler: authz_handler.clone(),
        };

        let objects_endpoints = ObjectServer {
            database_client: mongo_handler.clone(),
            object_handler: object_storage_handler.clone(),
            auth_handler: authz_handler.clone(),
        };

        let load_endpoints = LoadServer {
            mongo_client: mongo_handler.clone(),
            object_handler: object_storage_handler.clone(),
            auth_handler: authz_handler.clone(),
        };

        let endpoints = TestEndpointStruct {
            dataset_handler: dataset_endpoints,
            project_handler: project_endpoints,
            object_handler: objects_endpoints,
            load_handler: load_endpoints,
        };

        return endpoints;
    }

    async fn project_test(endpoints: &TestEndpointStruct) -> String {
        let create_project_request = Request::new(services::CreateProjectRequest {
            name: "testproject1".to_string(),
            description: "Some description".to_string(),
            ..Default::default()
        });

        let create_project_request_2 = Request::new(services::CreateProjectRequest {
            name: "testproject2".to_string(),
            description: "Some description".to_string(),
            ..Default::default()
        });

        let project = endpoints
            .project_handler
            .create_project(create_project_request)
            .await
            .unwrap()
            .into_inner();

        let _project_2 = endpoints
            .project_handler
            .create_project(create_project_request_2)
            .await
            .unwrap()
            .into_inner();

        let found_projects = endpoints
            .project_handler
            .get_user_projects(Request::new(models::Empty {}))
            .await
            .unwrap();

        if found_projects.get_ref().projects.len() != 2 {
            panic!("wrong number of projects found for testuser")
        };

        return project.id.clone();
    }

    async fn dataset_test(test_project_id: String, endpoints: &TestEndpointStruct) -> String {
        let create_dataset_request = Request::new(services::CreateDatasetRequest {
            project_id: test_project_id,
            name: "testdataset".to_string(),
            ..Default::default()
        });

        let dataset = endpoints
            .dataset_handler
            .create_dataset(create_dataset_request)
            .await
            .unwrap()
            .into_inner();

        endpoints
            .dataset_handler
            .get_dataset(Request::new(models::Id {
                id: dataset.id.clone(),
            }))
            .await
            .unwrap();

        return dataset.id.clone();
    }

    async fn object_group_test(test_dataset_id: String, endpoint: &TestEndpointStruct) -> String {
        let create_object_request = services::CreateObjectRequest {
            filename: "testobject.txt".to_string(),
            filetype: "txt".to_string(),
            content_len: 8,
            ..Default::default()
        };

        let create_object_group_request =
            Request::new(services::CreateObjectGroupWithRevisionRequest {
                object_group: Some(services::CreateObjectGroupRequest {
                    dataset_id: test_dataset_id,
                    name: "test_group".to_string(),
                    ..Default::default()
                }),
                object_group_version: Some(services::CreateObjectGroupRevisionRequest {
                    objects: vec![create_object_request],
                    ..Default::default()
                }),
                ..Default::default()
            });

        let object_group = endpoint
            .object_handler
            .create_object_group(create_object_group_request)
            .await
            .unwrap()
            .into_inner();

        let object_id = object_group.object_group_revision.unwrap().objects[0]
            .id
            .clone();

        let upload_request = Request::new(models::Id {
            id: object_id.clone(),
        });

        let upload_link = endpoint
            .load_handler
            .create_upload_link(upload_request)
            .await
            .unwrap()
            .into_inner();

        let client = reqwest::Client::new();
        let resp = client
            .put(upload_link.upload_link)
            .body(TEST_DATA_REV1.clone())
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

        let download_link = endpoint
            .load_handler
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

        if data_string != TEST_DATA_REV1 {
            panic!("downloaded data does not match uploaded rata")
        }

        return object_group.object_group.unwrap().id;
    }

    async fn test_revisions(
        endpoints: &TestEndpointStruct,
        object_group_id: String,
    ) -> Result<(), tonic::Status> {
        let create_object_request = services::CreateObjectRequest {
            filename: "testobject.txt".to_string(),
            filetype: "txt".to_string(),
            content_len: 8,
            ..Default::default()
        };

        let create_revision = services::CreateObjectGroupRevisionRequest {
            objects: vec![create_object_request],
            ..Default::default()
        };

        let add_revision_request = services::AddRevisionToObjectGroupRequest {
            object_group_id: object_group_id.clone(),
            group_version: Some(create_revision),
        };

        let added_revision = endpoints
            .object_handler
            .add_revision_to_object_group(Request::new(add_revision_request))
            .await?;
        let added_revision_ref = added_revision.get_ref();

        let object_id = added_revision_ref
            .object_group_revision
            .clone()
            .unwrap()
            .objects[0]
            .id
            .clone();

        load_test(object_id, endpoints, TEST_DATA_REV2).await;

        return Ok(());
    }

    async fn load_test(object_id: String, endpoints: &TestEndpointStruct, testdata: &'static str) {
        let upload_request = Request::new(models::Id {
            id: object_id.clone(),
        });

        let upload_link = endpoints
            .load_handler
            .create_upload_link(upload_request)
            .await
            .unwrap()
            .into_inner();

        let client = reqwest::Client::new();
        let resp = client
            .put(upload_link.upload_link)
            .body(TEST_DATA_REV1.clone())
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

        let download_link = endpoints
            .load_handler
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

        if data_string != testdata {
            panic!("downloaded data does not match uploaded rata")
        }
    }

    #[tokio::test]
    async fn full_test() {
        test_init();

        let endpoints = test_endpoint_structs().await;

        let project_id = project_test(&endpoints).await;
        let dataset_id = dataset_test(project_id, &endpoints).await;
        object_group_test(dataset_id.clone(), &endpoints).await;
    }
}
