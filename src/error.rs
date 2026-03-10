use std::io;
use bincode;
use serde_json;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Debug, Error)]
pub enum KvsError {
    #[error("invalid io: {}", _0)]
    Io(#[from] io::Error),
    #[error("failed io write to file: {}", _0)]
    Serde(#[from] serde_json::Error),
    #[error("Key not found")]
    KeyNotFound,
    #[error("unexpected command")]
    UnexpectedCommandType,
    #[error("couldnt find reader")]
    ReaderNotFound,
    #[error("invalid engine")]
    InvalidEngine,
    /// Bincode error
    #[error("bincode error: {}", _0)]
    Bincode(#[from] bincode::Error),
    /// Sled error
    #[error("sled error: {}", _0)]
    Sled(#[from] sled::Error),
    /// Error with a string message
    #[error("{}", _0)]
    StringError(String),
    /// Eof
    #[error("EOF")]
    EOF,
    /// ThreadPool
    #[error("threadpool error: {}", _0)]
    ThreadPool(String),
}