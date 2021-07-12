use std::sync::Arc;

use crate::{database::database::Database, objectstorage::objectstorage::StorageHandler};

use super::{
    create::CreateHandler, delete::DeleteHandler, load::LoadHandler, read::ReadHandler,
    update::UpdateHandler,
};

/// Handles the standard actions required by the API
/// This is a base struct that is aliased by the specific handler implementations for easier access
pub struct CommonHandler<T: Database + 'static> {
    pub database_client: Arc<T>,
    pub object_handler: Arc<dyn StorageHandler>,
}

impl<T: Database + 'static> CommonHandler<T> {
    pub async fn new(database_client: Arc<T>, object_storage: Arc<dyn StorageHandler>) -> Self {
        let common_handler = CommonHandler {
            database_client: database_client,
            object_handler: object_storage,
        };

        return common_handler;
    }
}

/// Wraps the specific handler into a single sturct
pub struct HandlerWrapper<T: Database + 'static> {
    pub create_handler: CreateHandler<T>,
    pub read_handler: ReadHandler<T>,
    pub update_handler: UpdateHandler<T>,
    pub delete_handler: DeleteHandler<T>,
    pub load_handler: LoadHandler<T>,
}

impl<T: Database + 'static> HandlerWrapper<T> {
    pub async fn new(
        database_client: Arc<T>,
        object_handler: Arc<dyn StorageHandler>,
    ) -> Result<Self, tonic::Status> {
        let handler_wrapper: HandlerWrapper<T> = HandlerWrapper {
            read_handler: ReadHandler {
                database_client: database_client.clone(),
                object_handler: object_handler.clone(),
            },
            update_handler: UpdateHandler {
                database_client: database_client.clone(),
                object_handler: object_handler.clone(),
            },
            delete_handler: DeleteHandler {
                database_client: database_client.clone(),
                object_handler: object_handler.clone(),
            },
            load_handler: LoadHandler {
                database_client: database_client.clone(),
                object_handler: object_handler.clone(),
            },
            create_handler: CreateHandler {
                database_client: database_client.clone(),
                object_handler: object_handler.clone(),
            },
        };

        return Ok(handler_wrapper);
    }
}
