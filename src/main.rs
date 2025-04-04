use std::io::{Read, Write};

use databas::lexer::Lexer;

fn run(buf: String) {
    let lexer = Lexer::new(&buf);
    for token in lexer {
        match token {
            Ok(tok) => println!("{tok}"),
            Err(err) => {
                eprintln!("{err}");
                break;
            }
        }
    }
    println!();
}

fn main() -> std::io::Result<()> {
    if let Some(file_arg) = std::env::args().nth(1) {
        let mut file = std::fs::File::open(file_arg)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        run(buf);
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
