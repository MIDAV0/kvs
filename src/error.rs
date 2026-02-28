use failure::Fail;
use std::io;
use bincode;
use serde_json;

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "invalid io: {}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "failed io write to file: {}", _0)]
    Serde(#[cause] serde_json::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "unexpected command")]
    UnexpectedCommandType,
    #[fail(display = "couldnt find reader")]
    ReaderNotFound,
    #[fail(display = "invalid engine")]
    InvalidEngine,
    /// Bincode error
    #[fail(display = "bincode error: {}", _0)]
    Bincode(#[cause] bincode::Error),
    /// Sled error
    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),
    /// Error with a string message
    #[fail(display = "{}", _0)]
    StringError(String),
    /// Eof
    #[fail(display = "EOF")]
    EOF,
    /// ThreadPool
    #[fail(display = "threadpool error: {}", _0)]
    ThreadPool(String),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        match err.kind() {
            io::ErrorKind::UnexpectedEof => KvsError::EOF,
            _ => KvsError::Io(err),
        }
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::Sled(err)
    }
}

impl From<bincode::Error> for KvsError {
    fn from(err: bincode::Error) -> KvsError {
        KvsError::Bincode(err)
    }
}