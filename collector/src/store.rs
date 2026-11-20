//! Persist small key-value state (cursors, tokens) as one JSON file on the volume.

#[cfg(test)]
mod tests;

use std::fs::{create_dir_all, read, rename, write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Error, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::tri;

/// The state file and its in-memory copy.
pub(crate) struct Store {
    /// Every key and its JSON value.
    data: Mutex<Map<String, Value>>,
    /// Where `flush` writes.
    path: PathBuf,
}

impl Store {
    /// Lock the data; a poisoned lock still holds a consistent map.
    fn data(&self) -> MutexGuard<'_, Map<String, Value>> {
        self.data.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Write the whole store to disk atomically.
    fn flush(&self, data: &Map<String, Value>) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            tri!(create_dir_all(dir));
        }
        let tmp = self.path.with_extension("json.tmp");
        tri!(write(&tmp, tri!(serde_json::to_vec_pretty(data))));
        tri!(rename(&tmp, &self.path).context("replace state file"));
        Ok(())
    }

    /// The value at `key`, if it exists and deserializes to `T`.
    pub(crate) fn get<T>(&self, key: &str) -> Option<T>
    where
        T: DeserializeOwned,
    {
        self.data()
            .get(key)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }

    /// Copy of the store with secrets replaced, for the admin endpoint.
    pub(crate) fn redacted(&self) -> Value {
        let data = self.data();
        let out = data
            .iter()
            .map(|(key, value)| {
                let secret =
                    key.contains("token") || key.contains("secret") || key.contains("oauth");
                let shown = if secret {
                    Value::String("<redacted>".into())
                } else {
                    value.clone()
                };
                (key.clone(), shown)
            })
            .collect();
        drop(data);
        Value::Object(out)
    }

    /// Remove `key` and write the whole store to disk atomically.
    ///
    /// # Errors
    /// The state file cannot be written.
    pub(crate) fn remove(&self, key: &str) -> Result<()> {
        let mut data = self.data();
        drop(data.remove(key));
        let flushed = self.flush(&data);
        drop(data);
        flushed
    }

    /// Set `key` and write the whole store to disk atomically.
    ///
    /// # Errors
    /// The value does not serialize or the state file cannot be written.
    pub(crate) fn set<T>(&self, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let json = tri!(serde_json::to_value(value));
        let mut data = self.data();
        drop(data.insert(key.to_owned(), json));
        let flushed = self.flush(&data);
        drop(data);
        flushed
    }
}

/// An empty store that `set` creates at the path.
impl From<PathBuf> for Store {
    fn from(path: PathBuf) -> Self {
        Self {
            data: Mutex::new(Map::new()),
            path,
        }
    }
}

/// Open the store at the path, reading the existing file if there is one.
impl TryFrom<&Path> for Store {
    type Error = Error;

    /// # Errors
    /// The file exists and cannot be read or parsed.
    fn try_from(path: &Path) -> Result<Self> {
        let exists = tri!(
            path.try_exists()
                .with_context(|| format!("read state file {}", path.display()))
        );
        if !exists {
            return Ok(Self::from(path.to_path_buf()));
        }
        let bytes = tri!(read(path).with_context(|| format!("read state file {}", path.display())));
        let data = tri!(
            serde_json::from_slice(&bytes)
                .with_context(|| format!("parse state file {}", path.display()))
        );
        Ok(Self {
            data: Mutex::new(data),
            path: path.to_path_buf(),
        })
    }
}
