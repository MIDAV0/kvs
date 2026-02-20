use kvs::{error::Result, client::KvClient};

use std::process::exit;
use clap::{Parser, Subcommand};


#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    /// IP address to listen at
    #[arg(long, global = true, default_value = "127.0.0.1:4000")]
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

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("{}", e);
        exit(1);
    } 
}


fn run(cli: Cli) -> Result<()> {
    
    match cli.command {
        Commands::Set { key, value } => {
            let mut client = KvClient::connect(cli.addr)?;
            client.set(key, value)?;
        }
        Commands::Get { key } => {
            let mut client = KvClient::connect(cli.addr)?;
            if let Some(val) = client.get(key)? {
                println!("{}", val);
            } else {
                println!("Key not found");
            }
        }
        Commands::Rm { key } => {
            let mut client = KvClient::connect(cli.addr)?;
            client.remove(key)?;
        }
    };
    Ok(()) 
}