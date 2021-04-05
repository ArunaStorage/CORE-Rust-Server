use async_trait::async_trait;

use super::common_models::{DatabaseModel, Label};

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[async_trait]
pub trait Database {
    async fn find_by_key<'de, T: DatabaseModel<'de>>(
        &self,
        key: String,
        value: String,
    ) -> ResultWrapper<Option<Vec<T>>>;
    async fn store<'de, T: DatabaseModel<'de>>(&self, value: T) -> ResultWrapper<T>;
}
