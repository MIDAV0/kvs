use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::net::TcpStream;

use crate::error::Result;

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Set(String,String),
    Get(String),
    Rm(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SetResponse {
    Ok(()),
    Err(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RemoveResponse {
    Ok(()),
    Err(String),
}

pub fn send_message<T: Serialize>(stream: &mut TcpStream, msg: &T) -> Result<()> {
    // 1. Serialize the message to a buffer
    let data = bincode::serialize(msg)?;

    // 2. Write the length prefix (u32, 4 bytes)
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes())?;

    // 3. Write the actual payload
    stream.write_all(&data)?;
    
    // Optional: flush to ensure it goes out immediately
    stream.flush()?; 
    Ok(())
}

pub fn recv_message<T: DeserializeOwned>(stream: &mut TcpStream) -> Result<T> {
    // 1. Read the length prefix (4 bytes)
    let mut len_buf = [0u8; 4];
    
    // read_exact is crucial: it blocks until exactly 4 bytes are read.
    // If the stream closes early, it returns an Error (UnexpectedEof).
    stream.read_exact(&mut len_buf)?; 
    
    let len = u32::from_be_bytes(len_buf) as usize;

    // 2. Allocate buffer of the exact expected size
    let mut buf = vec![0u8; len];

    // 3. Read the payload
    stream.read_exact(&mut buf)?;

    // 4. Deserialize
    let msg = bincode::deserialize(&buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(msg)
}
