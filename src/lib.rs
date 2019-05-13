//! A high performance Web Archive (WARC) file parser
//!
//! ## Usage
//!
//! ```rust
//! use rust_warc::WarcReader;
//!
//! use std::io;
//!
//! fn main() {
//!     let stdin = io::stdin();
//!     let handle = stdin.lock();
//!     let mut warc = WarcReader::new(handle);
//!
//!     let mut response_counter = 0;
//!     for item in warc {
//!         let record = item.unwrap(); // could be IO/Malformed error
//!         if record.header.get("WARC-Type") == Some(&String::from("response")) {
//!             response_counter += 1;
//!         }
//!     }
//!
//!     println!("# response records: {}", response_counter);
//! }
//! ```

use std::collections::HashMap;
use std::io::BufRead;

// trim a string in place (no (re)allocations)
fn ltrim(s: &mut String) {
    s.truncate(s.trim_end().len());
}

/// WARC Record
///
/// A record consists of the version string, a list of headers and the actual content (in bytes)
pub struct WarcRecord {
    /// WARC version string (WARC/1.1)
    pub version: String,
    /// Record header fields
    pub header: HashMap<String, String>,
    /// Record content block
    pub content: Vec<u8>,
}

/// WARC Processing error
#[derive(Debug)]
pub enum WarcError {
    Malformed(String),
    IO(std::io::Error),
}

/// WARC reader instance
///
/// The WarcReader serves as an iterator for [WarcRecords](WarcRecord) (or [errors](WarcError))
pub struct WarcReader<R> {
    read: R,
    valid_state: bool,
    linefeed: [u8; 4],
}

impl<R: BufRead> WarcReader<R> {
    /// Create a new WarcReader from a [BufRead] input
    pub fn new(read: R) -> Self {
        Self {
            read,
            valid_state: true,
            linefeed: [0u8; 4],
        }
    }
}

impl<R: BufRead> Iterator for WarcReader<R> {
    type Item = Result<WarcRecord, WarcError>;

    fn next(&mut self) -> Option<Result<WarcRecord, WarcError>> {
        if !self.valid_state {
            return None;
        }

        let mut version = String::new();

        if let Err(io) = self.read.read_line(&mut version) {
            self.valid_state = false;
            return Some(Err(WarcError::IO(io)));
        }

        if version.is_empty() {
            return None; // EOF
        }

        ltrim(&mut version);

        if !version.starts_with("WARC/1.") {
            self.valid_state = false;
            return Some(Err(WarcError::Malformed(String::from(
                "Unknown WARC version",
            ))));
        }

        let mut header = HashMap::with_capacity(16); // no allocations if <= 16 header fields

        loop {
            let mut line_buf = String::new();

            if let Err(io) = self.read.read_line(&mut line_buf) {
                self.valid_state = false;
                return Some(Err(WarcError::IO(io)));
            }

            if &line_buf == "\r\n" {
                break;
            }

            // todo field multiline continuations

            ltrim(&mut line_buf);

            if let Some(semi) = line_buf.find(':') {
                let value = line_buf.split_off(semi + 1).trim().to_string();
                line_buf.pop(); // eat colon
                ltrim(&mut line_buf);

                header.insert(line_buf, value);
            } else {
                self.valid_state = false;
                return Some(Err(WarcError::Malformed(String::from(
                    "Invalid header field",
                ))));
            }
        }

        let content_len = header.get("Content-Length");
        if content_len.is_none() {
            self.valid_state = false;
            return Some(Err(WarcError::Malformed(String::from(
                "Content-Length is missing",
            ))));
        }

        let content_len = content_len.unwrap().parse::<usize>();
        if content_len.is_err() {
            self.valid_state = false;
            return Some(Err(WarcError::Malformed(String::from(
                "Content-Length is not a number",
            ))));
        }

        let content_len = content_len.unwrap();
        let mut content = vec![0; content_len];
        if let Err(io) = self.read.read_exact(&mut content) {
            self.valid_state = false;
            return Some(Err(WarcError::IO(io)));
        }

        if let Err(io) = self.read.read_exact(&mut self.linefeed) {
            self.valid_state = false;
            return Some(Err(WarcError::IO(io)));
        }
        if self.linefeed != [13, 10, 13, 10] {
            self.valid_state = false;
            return Some(Err(WarcError::Malformed(String::from(
                "No double linefeed after record content",
            ))));
        }

        let record = WarcRecord {
            version,
            header,
            content,
        };

        Some(Ok(record))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn it_works() {
        let data = &include_bytes!("warc.in")[..];

        let mut warc = WarcReader::new(data);

        let item = warc.next();
        assert!(item.is_some());
        let item = item.unwrap();
        assert!(item.is_ok());
        let item = item.unwrap();
        assert_eq!(item.header.get("WARC-Type"), Some(&"warcinfo".to_string()));

        let item = warc.next();
        assert!(item.is_some());
        let item = item.unwrap();
        assert!(item.is_ok());
        let item = item.unwrap();
        assert_eq!(item.header.get("WARC-Type"), Some(&"request".to_string()));

        let item = warc.next();
        assert!(item.is_some());
        let item = item.unwrap();
        assert!(item.is_err()); // incomplete record
    }
}
