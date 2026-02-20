use std::net::TcpStream;
use clap::{Parser, Subcommand};

use crate::{
    error::{KvsError, Result},
    proto::{GetResponse, Request, SetResponse, RemoveResponse, recv_message, send_message}
};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    /// IP address to listen at
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Set (key, value)
    Set { key: String, value: String },
    /// Get key
    Get { key: String },
    /// Remove key
    Rm { key: String },
}

pub struct KvClient {
    stream: TcpStream,
}

impl KvClient {
    pub fn connect(addr: String) -> Result<KvClient> {
        let stream = TcpStream::connect(addr)?;
        Ok(KvClient { stream })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let req = Request::Get(key);
        send_message(&mut self.stream, &req)?;
        let resp: GetResponse = recv_message(&mut self.stream)?;
        
        match resp {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(err_msg) => Err(KvsError::StringError(err_msg)),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let req = Request::Set(key, value);
        send_message(&mut self.stream, &req)?;
        let resp: SetResponse = recv_message(&mut self.stream)?;
        
        match resp {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(err_msg) => Err(KvsError::StringError(err_msg)),
        }

    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let req = Request::Rm(key);
        send_message(&mut self.stream, &req)?;
        let resp: RemoveResponse = recv_message(&mut self.stream)?;

        match resp {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(err_msg) => Err(KvsError::StringError(err_msg)),
        }
    }
}
