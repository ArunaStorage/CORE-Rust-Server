#[cfg(test)]
mod server_test {
    use std::sync::Arc;

    use crate::{database, objectstorage};
    use crate::{database::database::Database, handler::common::CommonHandler};
    use crate::test_util::init;


    async fn init_common_handler() -> CommonHandler<database::mongo_connector::MongoHandler>{
        init::test_init();

        let uuid = uuid::Uuid::new_v4();

        let mongo_client = database::mongo_connector::MongoHandler::new_with_db_name(uuid.to_string()).await.unwrap();
        let s3_client = objectstorage::s3_objectstorage::S3Handler::new();

        let common_handler = CommonHandler::new(Arc::new(mongo_client), Arc::new(s3_client)).await;

        return common_handler;
    }
}