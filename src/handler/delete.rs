use crate::database::database::Database;

use super::common::CommonHandler;

pub type DeleteHandler<T: Database> = CommonHandler<T>;
