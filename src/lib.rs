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
//!         let record = item.expect("IO/malformed error");
//!
//!         // header names are case insensitive
//!         if record.header.get(&"WARC-Type".into()) == Some(&"response".into()) {
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
fn rtrim(s: &mut String) {
    s.truncate(s.trim_end().len());
}

/// Case insensitive string
///
/// ```
/// use rust_warc::CaseString;
///
/// // explicit constructor
/// let s1 = CaseString::from(String::from("HELLO!"));
///
/// // implicit conversion from String or &str
/// let s2: CaseString = "hello!".into();
///
/// assert_eq!(s1, s2);
/// ```
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct CaseString {
    inner: String,
}
impl CaseString {
    pub fn to_string(self) -> String {
        self.into()
    }
}

impl PartialEq<String> for CaseString {
    fn eq(&self, other: &String) -> bool {
        self.inner == other.to_ascii_lowercase()
    }
}

impl From<String> for CaseString {
    fn from(mut s: String) -> Self {
        s.make_ascii_lowercase();

        CaseString { inner: s }
    }
}
impl From<&str> for CaseString {
    fn from(s: &str) -> Self {
        String::from(s).into()
    }
}

impl Into<String> for CaseString {
    fn into(self) -> String {
        self.inner
    }
}

/// WARC Record
///
/// A record consists of the version string, a list of headers and the actual content (in bytes)
///
/// # Usage
/// ```rust
/// use rust_warc::WarcRecord;
///
/// /* test.warc:
/// WARC/1.1
/// WARC-Type: warcinfo
/// WARC-Date: 2006-09-19T17:20:14Z
/// WARC-Record-ID: <urn:uuid:d7ae5c10-e6b3-4d27-967d-34780c58ba39>
/// Content-Type: text/plain
/// Content-Length: 4
///
/// test
///
/// */
///
/// let mut data = &include_bytes!("test.warc")[..];
///
/// let item = WarcRecord::parse(&mut data).unwrap();
///
/// assert_eq!(item.version, "WARC/1.1");
///
/// // header names are case insensitive
/// assert_eq!(item.header.get(&"content-type".into()), Some(&"text/plain".into()));
///
/// assert_eq!(item.content, "test".as_bytes());
/// ```
pub struct WarcRecord {
    /// WARC version string (WARC/1.1)
    pub version: String,
    /// Record header fields
    pub header: HashMap<CaseString, String>,
    /// Record content block
    pub content: Vec<u8>,
}

impl WarcRecord {
    pub fn parse(mut read: impl BufRead) -> Result<Self, WarcError> {
        let mut version = String::new();

        if let Err(io) = read.read_line(&mut version) {
            return Err(WarcError::IO(io));
        }

        if version.is_empty() {
            return Err(WarcError::EOF);
        }

        rtrim(&mut version);

        if !version.starts_with("WARC/1.") {
            return Err(WarcError::Malformed(String::from("Unknown WARC version")));
        }

        let mut header = HashMap::<CaseString, String>::with_capacity(16); // no allocations if <= 16 header fields

        loop {
            let mut line_buf = String::new();

            if let Err(io) = read.read_line(&mut line_buf) {
                return Err(WarcError::IO(io));
            }

            // leniency: allow absent carriage return
            if &line_buf == "\r\n" || &line_buf == "\n" {
                break;
            }

            // todo field multiline continuations

            rtrim(&mut line_buf);

            if let Some(semi) = line_buf.find(':') {
                let value = line_buf.split_off(semi + 1).trim().to_string();
                line_buf.pop(); // eat colon
                rtrim(&mut line_buf);

                header.insert(line_buf.into(), value);
            } else {
                return Err(WarcError::Malformed(String::from("Invalid header field")));
            }
        }

        let content_len = header.get(&"Content-Length".into());
        if content_len.is_none() {
            return Err(WarcError::Malformed(String::from(
                "Content-Length is missing",
            )));
        }

        let content_len = content_len.unwrap().parse::<usize>();
        if content_len.is_err() {
            return Err(WarcError::Malformed(String::from(
                "Content-Length is not a number",
            )));
        }

        let content_len = content_len.unwrap();
        let mut content = vec![0; content_len];
        if let Err(io) = read.read_exact(&mut content) {
            return Err(WarcError::IO(io));
        }

        let mut linefeed = [0u8; 4];
        if let Err(io) = read.read_exact(&mut linefeed) {
            return Err(WarcError::IO(io));
        }
        if linefeed != [13, 10, 13, 10] {
            return Err(WarcError::Malformed(String::from(
                "No double linefeed after record content",
            )));
        }

        let record = WarcRecord {
            version,
            header,
            content,
        };

        Ok(record)
    }
}

/// WARC Processing error
#[derive(Debug)]
pub enum WarcError {
    Malformed(String),
    IO(std::io::Error),
    EOF,
}

/// WARC reader instance
///
/// The WarcReader serves as an iterator for [WarcRecords](WarcRecord) (or [errors](WarcError))
///
/// # Usage
/// ```rust
/// use rust_warc::{WarcReader, WarcRecord, WarcError};
///
/// let data = &include_bytes!("warc.in")[..];
/// let mut warc = WarcReader::new(data);
///
/// let item: Option<Result<WarcRecord, WarcError>> = warc.next();
/// assert!(item.is_some());
///
/// // count remaining items
/// assert_eq!(warc.count(), 2);
/// ```
pub struct WarcReader<R> {
    read: R,
    valid_state: bool,
}

impl<R: BufRead> WarcReader<R> {
    /// Create a new WarcReader from a [BufRead] input
    pub fn new(read: R) -> Self {
        Self {
            read,
            valid_state: true,
        }
    }
}

impl<R: BufRead> Iterator for WarcReader<R> {
    type Item = Result<WarcRecord, WarcError>;

    fn next(&mut self) -> Option<Result<WarcRecord, WarcError>> {
        if !self.valid_state {
            return None;
        }

        match WarcRecord::parse(&mut self.read) {
            Ok(item) => Some(Ok(item)),
            Err(WarcError::EOF) => None,
            Err(e) => {
                self.valid_state = false;
                Some(Err(e))
            }
        }
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
        assert_eq!(
            item.header.get(&"WARC-Type".into()),
            Some(&"warcinfo".into())
        );

        let item = warc.next();
        assert!(item.is_some());
        let item = item.unwrap();
        assert!(item.is_ok());
        let item = item.unwrap();
        assert_eq!(
            item.header.get(&"WARC-Type".into()),
            Some(&"request".into())
        );

        let item = warc.next();
        assert!(item.is_some());
        let item = item.unwrap();
        assert!(item.is_err()); // incomplete record
    }
}
