use std::sync::Arc;

use log::{info};
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::{
    dataset_objects_service_server::DatasetObjectsServiceServer,
    dataset_service_server::DatasetServiceServer, object_load_server::ObjectLoadServer,
    project_api_server::ProjectApiServer,
};
use tonic::{transport::Server};

use crate::objectstorage::s3_objectstorage::S3Handler;

use super::{
    dataset_api::DatasetsServer, load_api::LoadServer, object_api::ObjectServer,
    project_api::ProjectServer,
};

use crate::database::mongo_connector::MongoHandler;

use crate::SETTINGS;

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;


pub async fn start_server() -> ResultWrapper<()> {
    let s3_endpoint = SETTINGS
        .read()
        .unwrap()
        .get_str("Storage.Endpoint")
        .unwrap_or("localhost".to_string());
    let s3_bucket = SETTINGS.read().unwrap().get_str("Storage.Bucket").unwrap();
    let database_name = SETTINGS
        .read()
        .unwrap()
        .get_str("Database.Mongo.Database")
        .unwrap_or("test-database".to_string());

    let mongo_handler = Arc::new(MongoHandler::new(s3_bucket.clone()).await?);

    let object_storage_handler = Arc::new(S3Handler::new(
        s3_endpoint.to_string(),
        "RegionOne".to_string(),
        s3_bucket.clone(),
    ));

    let project_endpoints = ProjectServer {
        mongo_client: mongo_handler.clone(),
    };

    let dataset_endpoints = DatasetsServer {
        mongo_client: mongo_handler.clone(),
    };

    let objects_endpoints = ObjectServer {
        mongo_client: mongo_handler.clone(),
        object_handler: object_storage_handler.clone(),
    };

    let load_endpoints = LoadServer {
        mongo_client: mongo_handler.clone(),
        object_handler: object_storage_handler.clone(),
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
