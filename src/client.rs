use std::{
    env,
    io::{BufRead, Write, stdout},
    process,
};

use databas::{
    core::Database,
    error::DatabaseError,
    executor::{ExecutionOutput, Executor},
    planner::Planner,
    sql_parser::parser::Parser,
};

pub fn run() -> Result<(), DatabaseError<'static>> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "databas".to_owned());
    let cli = parse_args(args).unwrap_or_else(|()| usage(&program));

    let result: Result<Database, DatabaseError<'static>> =
        Database::open_or_create(&cli.path).map_err(DatabaseError::from);

    let db = match result {
        Ok(db) => db,
        Err(err) => {
            eprintln!("{err}");
            process::exit(1);
        }
    };

    match cli.mode {
        ClientMode::Repl => run_repl(db),
        ClientMode::Command(command) => run_command(db, command),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Cli {
    path: String,
    mode: ClientMode,
}

#[derive(Debug, PartialEq, Eq)]
enum ClientMode {
    Repl,
    Command(String),
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Cli, ()> {
    let mut args = args.into_iter();
    match args.next() {
        Some(flag) if flag == "-c" => {
            let command = args.next().ok_or(())?;
            let path = args.next().ok_or(())?;
            if args.next().is_some() {
                return Err(());
            }
            Ok(Cli { path, mode: ClientMode::Command(command) })
        }
        Some(path) if path.starts_with('-') => Err(()),
        Some(path) => {
            if args.next().is_some() {
                return Err(());
            }
            Ok(Cli { path, mode: ClientMode::Repl })
        }
        None => Err(()),
    }
}

fn run_repl(db: Database) -> Result<(), DatabaseError<'static>> {
    let planner = Planner::new(&db);
    let mut executor = Executor::new(&db);

    println!("Databas");

    let mut buf = String::new();
    let mut stdio = std::io::stdin().lock();
    loop {
        buf.clear();
        print!(">>> ");
        stdout().flush()?;
        let count = stdio.read_line(&mut buf)?;
        if count == 0 {
            break;
        }
        let buf = buf.trim_end();
        if buf.is_empty() {
            continue;
        }
        if buf == ".exit" {
            break;
        }
        let timer = std::time::Instant::now();
        let exec_res = execute_query(buf, &planner, &mut executor);
        match exec_res {
            Ok(output) => {
                let mut stdout = stdout();
                if let Err(err) = write_execution_output(output, &mut stdout) {
                    if matches!(&err, DatabaseError::Io(_)) {
                        return Err(err);
                    }
                    eprintln!("{err}");
                }
            }
            Err(err) => eprintln!("{err}"),
        };
        let elapsed = timer.elapsed();
        println!("Executed query in {elapsed:?}.");
    }
    db.flush()?;
    Ok(())
}

fn run_command(db: Database, command: String) -> Result<(), DatabaseError<'static>> {
    let command = command_with_trailing_semicolon(command);
    let planner = Planner::new(&db);
    let mut executor = Executor::new(&db);

    match execute_query(&command, &planner, &mut executor) {
        Ok(output) => {
            let mut stdout = stdout();
            if let Err(err) = write_execution_output(output, &mut stdout) {
                if matches!(&err, DatabaseError::Io(_)) {
                    return Err(err);
                }
                eprintln!("{err}");
                db.flush()?;
                process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("{err}");
            db.flush()?;
            process::exit(1);
        }
    }

    db.flush()?;
    Ok(())
}

fn command_with_trailing_semicolon(mut command: String) -> String {
    if !command.trim_end().ends_with(';') {
        command.push(';');
    }
    command
}

fn execute_query<'a>(
    query: &'a str,
    planner: &Planner<'_>,
    executor: &mut Executor<'_>,
) -> Result<ExecutionOutput, DatabaseError<'a>> {
    let query = Parser::new(query).stmt()?;
    let plan = planner.plan_statement(&query)?;
    let output = executor.execute(plan.physical)?;
    Ok(output)
}

fn write_execution_output(
    output: ExecutionOutput,
    writer: &mut impl Write,
) -> Result<(), DatabaseError<'static>> {
    match output {
        ExecutionOutput::Rows { rows } => {
            for row in rows {
                writeln!(writer, "{}", row?)?;
            }
        }
        output => writeln!(writer, "{output}")?,
    }
    Ok(())
}

fn usage(program: &str) -> ! {
    eprintln!("usage: {program} [-c COMMAND] <database-file>");
    process::exit(2);
}
