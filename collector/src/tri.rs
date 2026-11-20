//! Early return on `Err`, spelled out because the `?` operator is linted against.

/// Unwrap a `Result`, or return its error converted into the caller's error type.
#[macro_export]
macro_rules! tri {
    ($expression:expr) => {
        match $expression {
            Ok(value) => value,
            Err(error) => return Err(error.into()),
        }
    };
}
