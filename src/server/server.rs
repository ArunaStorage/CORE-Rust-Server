use std::sync::Arc;

use log::info;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::{
    dataset_objects_service_server::DatasetObjectsServiceServer,
    dataset_service_server::DatasetServiceServer, object_load_server::ObjectLoadServer,
    project_api_server::ProjectApiServer,
};
use tonic::transport::Server;

use crate::objectstorage::s3_objectstorage::S3Handler;

use crate::auth::{
    authenticator::AuthHandler, project_authorization_handler::ProjectAuthzHandler,
    test_authenticator::TestAuthenticator,
};

use super::{
    dataset_api::DatasetsServer, load_api::LoadServer, object_api::ObjectServer,
    project_api::ProjectServer,
};

use crate::database::mongo_connector::MongoHandler;

use crate::SETTINGS;

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Starts the grpc server. The configuration is read from the config file handed over at startup
pub async fn start_server() -> ResultWrapper<()> {
    let s3_endpoint = SETTINGS
        .read()
        .unwrap()
        .get_str("Storage.Endpoint")
        .unwrap_or("localhost".to_string());
    let s3_bucket = SETTINGS.read().unwrap().get_str("Storage.Bucket").unwrap();

    let mongo_handler = Arc::new(MongoHandler::new().await?);

    let object_storage_handler = Arc::new(S3Handler::new(
        s3_endpoint.to_string(),
        "RegionOne".to_string(),
        s3_bucket.clone(),
    ));

    let auth_type_handler = SETTINGS.read().unwrap().get_str("Authentication.Type")?;
    let auth_type_handler_str = auth_type_handler.as_str();

    let project_authz_handler: Arc<dyn AuthHandler> = match auth_type_handler_str {
        "debug" => Arc::new(TestAuthenticator {}),
        "oauth2" => Arc::new(ProjectAuthzHandler::new(mongo_handler.clone())?),
        _ => panic!("Could not parse auth type: {}", auth_type_handler),
    };

    let project_endpoints = ProjectServer {
        mongo_client: mongo_handler.clone(),
        auth_handler: project_authz_handler.clone(),
    };

    let dataset_endpoints = DatasetsServer {
        database_client: mongo_handler.clone(),
        auth_handler: project_authz_handler.clone(),
    };

    let objects_endpoints = ObjectServer {
        database_client: mongo_handler.clone(),
        object_handler: object_storage_handler.clone(),
        auth_handler: project_authz_handler.clone(),
    };

    let load_endpoints = LoadServer {
        mongo_client: mongo_handler.clone(),
        object_handler: object_storage_handler.clone(),
        auth_handler: project_authz_handler.clone(),
    };

    let host = SETTINGS.try_read().unwrap().get_str("Server.Host").unwrap();
    let port = SETTINGS.try_read().unwrap().get_int("Server.Port").unwrap();

    let addr = format!("{}:{}", &host, &port).parse()?;

    info!("Starting webserver on {} port {}", &host, &port);

    Server::builder()
        .add_service(ProjectApiServer::new(project_endpoints))
        .add_service(DatasetServiceServer::new(dataset_endpoints))
        .add_service(DatasetObjectsServiceServer::new(objects_endpoints))
        .add_service(ObjectLoadServer::new(load_endpoints))
        .serve(addr)
        .await?;

    Ok(())
}
