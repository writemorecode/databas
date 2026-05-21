use std::{env, process};

use databas::core::Database;

fn main() {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "databas".to_owned());
    let command = args.next();
    let path = args.next();

    if args.next().is_some() {
        usage(&program);
    }

    let Some(command) = command else {
        usage(&program);
    };
    let Some(path) = path else {
        usage(&program);
    };

    let result = match command.as_str() {
        "create" => Database::create(&path).and_then(|database| database.flush()),
        "open" => Database::open(&path).and_then(|database| database.flush()),
        _ => usage(&program),
    };

    if let Err(error) = result {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn usage(program: &str) -> ! {
    eprintln!("usage: {program} create|open <database-file>");
    process::exit(2);
}
