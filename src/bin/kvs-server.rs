use std::{
    env::current_dir, fs::{self, OpenOptions}, net::SocketAddr, process::exit
};
use clap::Parser;
use slog::{o, Drain, Logger};

use kvs::{engine::{KvsEngine, kvs::KvStore, sled::SledKvsEngine}, error::Result, server::KvServer, thread_pool::{ThreadPool, NaiveThreadPool}};


#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// IP address to listen at
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,
    /// Engine name
    #[arg(long, default_value = "kvs")]
    engine: String,
    /// Number of threads
    #[arg(long, default_value = "4")]
    threads: u32,
}

fn create_logger() -> Logger {
    let stderr_decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let stderr_drain = slog_term::CompactFormat::new(stderr_decorator).build().fuse();

    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("server-logs/kvs-server.log")
        .unwrap();
    let file_decorator = slog_term::PlainDecorator::new(log_file);
    let file_drain = slog_term::FullFormat::new(file_decorator).build().fuse();

    let combined = slog::Duplicate::new(stderr_drain, file_drain).fuse();
    
    let drain = slog_async::Async::new(combined).build().fuse();

    Logger::root(drain, o!("version" => "1"))
}

fn main() {
    let log = create_logger();
    let _guard = slog_scope::set_global_logger(log);
    
    let cli = Cli::parse();

    let res = current_engine().and_then(move |curr_engine| {
        if curr_engine.is_some() && Some(cli.engine.clone()) != curr_engine {
            slog::error!(slog_scope::logger(), "Engine mismatch"; "current_engine" => &curr_engine.unwrap(), "requested_engine" => &cli.engine);
            exit(1);
        }
        run(cli)
    });

    if let Err(e) = res {
        slog::error!(slog_scope::logger(), "Server error"; "error" => format!("{:?}", e));
        exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    slog::info!(slog_scope::logger(), "kvs-server"; "version" => env!("CARGO_PKG_VERSION"));
    slog::info!(slog_scope::logger(), "Storage engine"; "engine" => &cli.engine);    slog::info!(slog_scope::logger(), "Listening on"; "addr" => &cli.addr);

    let data_file = current_dir()?.join("data");

    fs::write(data_file.join("engine"), format!("{}", cli.engine))?;


    let pool = NaiveThreadPool::new(cli.threads)?;
    slog::info!(slog_scope::logger(), "Started a thread pool"; "threads" => &cli.threads);

    match cli.engine.as_str() {
        "kvs" => run_with_engine(KvStore::open(data_file)?, pool, cli.addr),
        "sled" => run_with_engine(SledKvsEngine::new(sled::open(data_file)?), pool, cli.addr),
        _ => {
            slog::error!(slog_scope::logger(), "Unsupported engine"; "engine" => &cli.engine);
            exit(1);
        }
    }
}

fn run_with_engine<E: KvsEngine, P: ThreadPool>(engine: E, pool: P, addr: SocketAddr) -> Result<()> {
    let server = KvServer::new(engine, pool);
    server.start(addr)
}

fn current_engine() -> Result<Option<String>> {
    let engine = current_dir()?.join("data").join("engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            slog::warn!(slog_scope::logger(), "The content of engine file is invalid"; "error" => format!("{:?}", e));
            Ok(None)
        }
    }
}