use databas::parser::Parser;
use std::io::Write;

fn run(buf: String) {
    let parser = Parser::new(&buf);
    for tree in parser {
        match tree {
            Ok(t) => println!("'{t}'"),
            Err(err) => {
                eprintln!("ERROR: {err}");
                return;
            }
        }
    }
    println!();
}

fn main() -> std::io::Result<()> {
    if let Some(query) = std::env::args().nth(1) {
        run(query);
        return Ok(());
    }

    let mut buf = String::new();
    let stdin = std::io::stdin();
    loop {
        buf.clear();
        print!(">>> ");
        std::io::stdout().flush()?;
        stdin.read_line(&mut buf)?;
        let line = buf.trim_end();
        if line == "exit" {
            break;
        }
        run(line.to_string());
    }

    Ok(())
}
