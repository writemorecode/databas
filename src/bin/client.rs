use std::{
    env,
    error::Error,
    io::{self, BufRead, Write},
    process,
    time::Instant,
};

use databas::client::{Client, ClientError, QueryResult};

const DEFAULT_ADDRESS: &str = "127.0.0.1:5432";

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "client".to_owned());
    let cli = parse_args(args).unwrap_or_else(|()| usage(&program));
    let mut client = Client::connect(&cli.address, &cli.database_name)?;

    match cli.command {
        Some(command) => {
            let command = command_with_trailing_semicolon(command);
            write_query_result(client.execute(&command)?, &mut io::stdout())?;
        }
        None => run_repl(&mut client)?,
    }
    Ok(())
}

struct Cli {
    address: String,
    database_name: String,
    command: Option<String>,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Cli, ()> {
    let mut args = args.into_iter();
    let mut address = DEFAULT_ADDRESS.to_owned();
    let mut command = None;
    let mut database_name = None;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--address" => address = args.next().ok_or(())?,
            "-c" | "--command" => command = Some(args.next().ok_or(())?),
            _ if argument.starts_with('-') => return Err(()),
            _ if database_name.is_none() => database_name = Some(argument),
            _ => return Err(()),
        }
    }

    Ok(Cli { address, database_name: database_name.ok_or(())?, command })
}

fn run_repl(client: &mut Client) -> Result<(), Box<dyn Error>> {
    println!("Databas");
    let mut input = io::stdin().lock();
    let mut buffer = String::new();

    loop {
        buffer.clear();
        print!(">>> ");
        io::stdout().flush()?;
        if input.read_line(&mut buffer)? == 0 {
            break;
        }
        let sql = buffer.trim_end();
        if sql.is_empty() {
            continue;
        }
        if sql == ".exit" {
            break;
        }

        let timer = Instant::now();
        match client.execute(sql) {
            Ok(result) => write_query_result(result, &mut io::stdout())?,
            Err(ClientError::Server(error)) => eprintln!("{error}"),
            Err(error) => return Err(error.into()),
        }
        println!("Executed query in {:?}.", timer.elapsed());
    }
    Ok(())
}

fn write_query_result(result: QueryResult, writer: &mut impl Write) -> io::Result<()> {
    match result {
        QueryResult::Rows(rows) => {
            for row in rows {
                for (index, value) in row.iter().enumerate() {
                    if index != 0 {
                        write!(writer, "\t")?;
                    }
                    write!(writer, "{value}")?;
                }
                writeln!(writer)?;
            }
        }
        result => writeln!(writer, "{result}")?,
    }
    Ok(())
}

fn command_with_trailing_semicolon(mut command: String) -> String {
    if !command.trim_end().ends_with(';') {
        command.push(';');
    }
    command
}

fn usage(program: &str) -> ! {
    eprintln!(
        "usage: {program} [--address HOST:PORT] [-c COMMAND] <database-name>\n\
         default address: {DEFAULT_ADDRESS}"
    );
    process::exit(2);
}
