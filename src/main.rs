use std::io::Write;

use databas::parser::Parser;

fn run(buf: String) {
    //let lexer = Lexer::new(&buf);
    let parser = Parser::new(&buf);
    let exp = parser.expr();
    match exp {
        Ok(expr) => {
            dbg!(expr);
        }
        Err(err) => println!("Error: {}", err),
    };
    println!();
}

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
        run(line.to_string());
    }

    Ok(())
}
