
use std::{
    net::{SocketAddr, TcpListener, TcpStream},
};

use crate::{
    engine::KvsEngine,
    error::{KvsError, Result},
    proto::{GetResponse, RemoveResponse, Request, SetResponse, recv_message, send_message}, thread_pool::ThreadPool
};


pub struct KvServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvServer<E, P> {
    pub fn new(engine: E, pool: P) -> Self {
        KvServer { engine, pool }
    }

    pub fn start(&self, addr: SocketAddr) -> Result<()> {

        let listener = TcpListener::bind(addr)?;

        slog::info!(slog_scope::logger(), "Listening to connections");

        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(move || {
                match stream {
                    Ok(stream) => {
                        slog::info!(slog_scope::logger(), "Client connected"; "client_addr" => &stream.peer_addr().unwrap());
                        if let Err(e) = handle_stream(stream, engine) {
                            slog::error!(slog_scope::logger(), "Request failed"; "err" => format!("{:?}", e));
                        }
                    }
                    Err(e) => {
                        slog::error!(slog_scope::logger(), "Client failed to connect"; "err" => format!("{:?}", e));
                    }
                } 
            });
        }

        Ok(())
    }
}

fn handle_stream<E: KvsEngine>(mut stream: TcpStream, engine: E) -> Result<()> {
    let peer_addr = stream.peer_addr()?;

    let req: Request = recv_message(&mut stream)?;

    slog::info!(slog_scope::logger(), "Received request"; "req" => format!("{:?}", req), "from" => peer_addr);

    macro_rules! send_resp {
        ($resp:expr) => {{
            let resp = $resp;
            send_message(&mut stream, &resp)?;
            slog::debug!(slog_scope::logger(), "Response sent"; "resp" => format!("{:?}", resp), "to" => peer_addr);
        }};
    }

    match req {
        Request::Get(k) => send_resp!(match engine.get(k) {
            Ok(item) => GetResponse::Ok(item),
            Err(e) => GetResponse::Err(format!("{}", e)),
        }),
        Request::Set(k, v) => send_resp!(match engine.set(k, v) {
            Ok(_) => SetResponse::Ok(()),
            Err(e) => SetResponse::Err(format!("{}", e)),
        }),
        Request::Rm(k) => send_resp!(match engine.remove(k) {
            Ok(_) => RemoveResponse::Ok(()),
            Err(e) => RemoveResponse::Err(format!("{}", e)),
        }),
    };

    Ok(())
}
