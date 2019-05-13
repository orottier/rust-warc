use rust_warc::WarcReader;

use std::io;

fn main() {
    let stdin = io::stdin();
    let handle = stdin.lock();
    let warc = WarcReader::new(handle);

    println!("Records: {}", warc.count());
}
