use std::io::Write;

use databas::lex::Lexer;

fn main() -> std::io::Result<()> {
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
        let lexer = Lexer::new(line);
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

    Ok(())
}
