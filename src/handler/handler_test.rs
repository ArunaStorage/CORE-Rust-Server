#[cfg(test)]
mod server_test {
    use std::sync::Arc;

    use scienceobjectsdb_rust_api::sciobjectsdbapi::models::v1::Version;
    use scienceobjectsdb_rust_api::sciobjectsdbapi::services;
    use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::ReleaseDatasetVersionRequest;

    use crate::handler::common::CommonHandler;
    use crate::models::common_models::DatabaseModel;
    use crate::models::dataset_model::DatasetEntry;
    use crate::models::dataset_object_group::ObjectGroupRevision;
    use crate::models::dataset_version::DatasetVersion;
    use crate::test_util::init;
    use crate::{database, objectstorage};

    async fn init_common_handler_for_test() -> CommonHandler<database::mongo_connector::MongoHandler>
    {
        init::test_init();

        let uuid = uuid::Uuid::new_v4();

        let mongo_client =
            database::mongo_connector::MongoHandler::new_with_db_name(uuid.to_string())
                .await
                .unwrap();
        let s3_client = objectstorage::s3_objectstorage::S3Handler::new();

        let common_handler = CommonHandler::new(Arc::new(mongo_client), Arc::new(s3_client)).await;

        return common_handler;
    }

    #[tokio::test]
    async fn dataset_test() {
        let handler = init_common_handler_for_test().await;
        let dataset_request = services::v1::CreateDatasetRequest {
            ..Default::default()
        };

        let created_dataset = handler.create_dataset(&dataset_request).await.unwrap();
        let read_dataset = handler
            .read_entry_by_id::<DatasetEntry>(created_dataset.id.as_str())
            .await
            .unwrap();
        assert_eq!(read_dataset, created_dataset);

        handler
            .delete_dataset(created_dataset.id.clone())
            .await
            .unwrap();

        let value = handler
            .read_entry_by_id::<DatasetEntry>(created_dataset.id.as_str())
            .await
            .unwrap_err();
        let expected_error = tonic::Status::not_found(format!(
            "could not find requested document. type: {}",
            DatasetEntry::get_model_name().unwrap()
        ));
        assert_eq!(value.code(), expected_error.code());
        assert_eq!(value.message(), expected_error.message());
    }

    #[tokio::test]
    async fn dataset_version() {
        let handler = init_common_handler_for_test().await;
        let dataset_request = services::v1::CreateDatasetRequest {
            ..Default::default()
        };
        let created_dataset = handler.create_dataset(&dataset_request).await.unwrap();

        let object_group = services::v1::CreateObjectGroupRequest {
            dataset_id: created_dataset.id.clone(),
            ..Default::default()
        };

        let created_object_group = handler.create_object_group(&object_group).await.unwrap();

        let object1 = services::v1::CreateObjectRequest {
            content_len: 3,
            filename: "testfile1.bin".to_string(),
            filetype: "bin".to_string(),
            ..Default::default()
        };

        let object2 = services::v1::CreateObjectRequest {
            content_len: 5,
            filename: "testfile2.bin".to_string(),
            filetype: "bin".to_string(),
            ..Default::default()
        };

        let revision_request = services::v1::CreateObjectGroupRevisionRequest {
            objects: vec![object1, object2],
            ..Default::default()
        };

        let inserted_revision1 = handler
            .create_revision_for_group(&revision_request, created_object_group.id.as_str())
            .await
            .unwrap();
        let inserted_revision2 = handler
            .create_revision_for_group(&revision_request, created_object_group.id.as_str())
            .await
            .unwrap();

        let release_version_request = ReleaseDatasetVersionRequest {
            dataset_id: created_dataset.id,
            revision_ids: vec![inserted_revision1.id, inserted_revision2.id],
            version: Some(Version {
                ..Default::default()
            }),
            ..Default::default()
        };

        let version = handler
            .create_datatset_version(&release_version_request)
            .await
            .unwrap();
        let read_version = handler
            .read_entry_by_id::<DatasetVersion>(version.id.as_str())
            .await
            .unwrap();

        assert_eq!(version, read_version);

        handler
            .delete_dataset_version(version.id.clone())
            .await
            .unwrap();

        let read_version_error = handler
            .read_entry_by_id::<DatasetVersion>(version.id.as_str())
            .await
            .unwrap_err();

        let expected_error = tonic::Status::not_found(format!(
            "could not find requested document. type: {}",
            DatasetVersion::get_model_name().unwrap()
        ));

        assert_eq!(read_version_error.code(), expected_error.code());
        assert_eq!(read_version_error.message(), expected_error.message());
    }

    #[tokio::test]
    async fn dataset_revision() {
        let handler = init_common_handler_for_test().await;
        let dataset_request = services::v1::CreateDatasetRequest {
            ..Default::default()
        };
        let created_dataset = handler.create_dataset(&dataset_request).await.unwrap();

        let object_group = services::v1::CreateObjectGroupRequest {
            dataset_id: created_dataset.id.clone(),
            ..Default::default()
        };

        let created_object_group = handler.create_object_group(&object_group).await.unwrap();

        let object1 = services::v1::CreateObjectRequest {
            content_len: 3,
            filename: "testfile1.bin".to_string(),
            filetype: "bin".to_string(),
            ..Default::default()
        };

        let object2 = services::v1::CreateObjectRequest {
            content_len: 5,
            filename: "testfile2.bin".to_string(),
            filetype: "bin".to_string(),
            ..Default::default()
        };

        let revision_request = services::v1::CreateObjectGroupRevisionRequest {
            objects: vec![object1, object2],
            ..Default::default()
        };

        let inserted_revision1 = handler
            .create_revision_for_group(&revision_request, created_object_group.id.as_str())
            .await
            .unwrap();
        let inserted_revision2 = handler
            .create_revision_for_group(&revision_request, created_object_group.id.as_str())
            .await
            .unwrap();

        handler
            .delete_object_revision(inserted_revision1.id.clone())
            .await
            .unwrap();

        let read_revision_error = handler
            .read_entry_by_id::<ObjectGroupRevision>(inserted_revision1.id.as_str())
            .await
            .unwrap_err();
        let expected_error = tonic::Status::not_found(format!(
            "could not find requested document. type: {}",
            ObjectGroupRevision::get_model_name().unwrap()
        ));

        assert_eq!(read_revision_error.code(), expected_error.code());
        assert_eq!(read_revision_error.message(), expected_error.message());

        let read_revision = handler
            .read_entry_by_id::<ObjectGroupRevision>(inserted_revision2.id.as_str())
            .await
            .unwrap();

        assert_eq!(inserted_revision2, read_revision)
    }
}
