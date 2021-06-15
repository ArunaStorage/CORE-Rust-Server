use crate::models::common_models::DatabaseModel;

// A simple helper function to turn an option value into a tonic error. This can be used to check if a required field
// that is defined as optional in the gRPC API is present in a request. Can be used to remove some boilerplate code.
// The fieldname is used for the error message to indicate which field was missing.
pub fn tonic_error_if_not_exists<'a, T>(
    option_value: &'a Option<T>,
    fieldname: &str,
) -> Result<&'a T, tonic::Status> {
    let value = match option_value {
        Some(value) => value,
        None => {
            return Err(tonic::Status::invalid_argument(format!(
                "field {} required",
                fieldname
            )))
        }
    };

    return Ok(value);
}

pub fn tonic_error_if_value_not_found<'a, 'de, T: DatabaseModel<'de>>(
    option_value: &'a Option<T>,
    id: &str,
) -> Result<&'a T, tonic::Status> {
    let value = match option_value {
        Some(value) => value,
        None => {
            return Err(tonic::Status::invalid_argument(format!(
                "could not find {} with id {}",
                T::get_model_name()?,
                id
            )))
        }
    };

    return Ok(value);
}
