mod client;

use databas::error::DatabaseError;

fn main() -> Result<(), DatabaseError<'static>> {
    client::run()
}
