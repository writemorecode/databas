use std::{env, error::Error, net::TcpListener, process};

use databas::{core::Database, server::Server};

const DEFAULT_ADDRESS: &str = "127.0.0.1:5432";

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "server".to_owned());
    let cli = parse_args(args).unwrap_or_else(|()| usage(&program));

    // Opening performs WAL recovery. Do this before binding so clients cannot
    // connect to a database that is not ready yet.
    let database = Database::open_or_create(&cli.database_file)?;
    let listener = TcpListener::bind(&cli.address)?;
    let local_address = listener.local_addr()?;
    let server = Server::new(listener, database, cli.database_name)?;

    println!("Databas server listening on {local_address}");
    server.serve()?;
    Ok(())
}

struct Cli {
    address: String,
    database_name: String,
    database_file: String,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Cli, ()> {
    let mut args = args.into_iter();
    let mut address = DEFAULT_ADDRESS.to_owned();
    let mut positional = Vec::with_capacity(2);

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--address" => address = args.next().ok_or(())?,
            _ if argument.starts_with('-') => return Err(()),
            _ => positional.push(argument),
        }
    }
    if positional.len() != 2 {
        return Err(());
    }

    Ok(Cli { address, database_name: positional.remove(0), database_file: positional.remove(0) })
}

fn usage(program: &str) -> ! {
    eprintln!(
        "usage: {program} [--address HOST:PORT] <database-name> <database-file>\n\
         default address: {DEFAULT_ADDRESS}"
    );
    process::exit(2);
}
