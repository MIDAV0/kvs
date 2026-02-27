
use std::{
    net::{TcpListener, TcpStream, SocketAddr},
};

use crate::{
    engine::KvsEngine,
    error::{KvsError, Result},
    proto::{GetResponse, RemoveResponse, Request, SetResponse, recv_message, send_message}
};


pub struct KvServer<E: KvsEngine> {
    engine: E
}

impl<E: KvsEngine> KvServer<E> {
    pub fn new(engine: E) -> Self {
        KvServer { engine }
    }

    pub fn start(&mut self, addr: SocketAddr) -> Result<()> {

        let listener = TcpListener::bind(addr)?;

        slog::info!(slog_scope::logger(), "Listening to connections");

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    slog::info!(slog_scope::logger(), "Client connected"; "client_addr" => &stream.peer_addr()?);
                    self.handle_stream(stream)?
                }
                Err(e) => {
                    slog::error!(slog_scope::logger(), "Client failed to connect"; "err" => format!("{:?}", e));
                }
            }
        }

        Ok(())
    }

    fn handle_stream(&mut self, mut stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr()?;

        loop {            
            let req: Request = match recv_message(&mut stream) {
                Ok(req) => req,
                Err(e) if matches!(e, KvsError::EOF) => {
                    slog::info!(slog_scope::logger(), "Client disconnected"; "client_addr" => peer_addr);
                    break;
                },
                Err(e) => {
                    slog::error!(slog_scope::logger(), "Failed to receive message"; "err" => format!("{:?}", e), "from" => peer_addr);
                    break;
                }
            };

            slog::info!(slog_scope::logger(), "Received request"; "req" => format!("{:?}", req), "from" => peer_addr);

            macro_rules! send_resp {
                ($resp:expr) => {{
                    let resp = $resp;
                    send_message(&mut stream, &resp)?;
                    slog::debug!(slog_scope::logger(), "Response sent"; "resp" => format!("{:?}", resp), "to" => peer_addr);
                }};
            }

            match req {
                Request::Get(k) => send_resp!(match self.engine.get(k) {
                    Ok(item) => GetResponse::Ok(item),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                }),
                Request::Set(k, v) => send_resp!(match self.engine.set(k, v) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                }),
                Request::Rm(k) => send_resp!(match self.engine.remove(k) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                }),
            };
        }

        Ok(())
    }
}