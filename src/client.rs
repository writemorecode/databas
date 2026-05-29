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
    let path = args.next();

    if args.next().is_some() {
        usage(&program);
    }

    let Some(path) = path else {
        usage(&program);
    };

    let result: Result<Database, DatabaseError<'static>> =
        Database::open_or_create(&path).map_err(DatabaseError::from);

    let db = match result {
        Ok(db) => db,
        Err(err) => {
            eprintln!("{err}");
            process::exit(1);
        }
    };
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
    eprintln!("usage: {program} <database-file>");
    process::exit(2);
}
