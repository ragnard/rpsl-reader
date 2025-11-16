use std::io::BufRead;

use thiserror::Error;

/// Callback trait for RPSL parsing events
pub trait Callbacks {
    /// Called when a new object starts
    fn start_object(&mut self);

    /// Called for each attribute with its name and value
    fn attribute(&mut self, name: &[u8], value: &[u8]);

    /// Called when an object ends
    fn end_object(&mut self);
}

/// No-op implementation of callbacks
pub struct Noop;

impl Callbacks for Noop {
    fn start_object(&mut self) {}
    fn attribute(&mut self, _name: &[u8], _value: &[u8]) {}
    fn end_object(&mut self) {}
}

/// Debug printer implementation of callbacks
pub struct Printer;

impl Callbacks for Printer {
    fn start_object(&mut self) {
        println!("<object>");
    }

    fn attribute(&mut self, name: &[u8], value: &[u8]) {
        println!(
            "  <attribute name=\"{}\">{}</attribute>",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(value)
        );
    }

    fn end_object(&mut self) {
        println!("</object>");
    }
}

/// RPSL Parser
pub struct RpslParser<C> {
    callbacks: C,
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unexpected end of file at line {line_number}")]
    UnexpectedEof { line_number: u32 },

    #[error("InvalidSyntax: {message} at line {line_number}: {line}")]
    InvalidSyntax {
        line_number: u32,
        message: &'static str,
        line: String,
    },
}

impl<C: Callbacks> RpslParser<C> {
    pub fn new(callbacks: C) -> Self {
        Self { callbacks }
    }

    pub fn into_callbacks(self) -> C {
        self.callbacks
    }

    pub fn parse<R: BufRead>(&mut self, mut reader: R) -> Result<(), ParseError> {
        let mut buf = Vec::with_capacity(8192);
        let mut cont_buf = Vec::with_capacity(8192);
        let mut in_object = false;
        let mut line_number = 0;

        loop {
            buf.clear();

            let Some(line) = Self::read_line(&mut reader, &mut buf)? else {
                if in_object {
                    self.callbacks.end_object();
                }
                return Ok(());
            };
            line_number += 1;

            let Some(clean_line) = Self::strip_comment(line) else {
                continue;
            };

            if clean_line.is_empty() {
                if in_object {
                    self.callbacks.end_object();
                    in_object = false;
                }
                continue;
            }

            if Self::is_continuation(clean_line[0]) {
                return Err(ParseError::InvalidSyntax {
                    line_number,
                    message: "Unexpected continuation line",
                    line: String::from_utf8_lossy(line).into(),
                });
            }

            let Some(colon_pos) = memchr::memchr(b':', clean_line) else {
                // Handle special EOF literal found in APNIC files
                if clean_line == [b'E', b'O', b'F'] {
                    return Ok(());
                }

                return Err(ParseError::InvalidSyntax {
                    line_number,
                    message: "Expected an attribute",
                    line: String::from_utf8_lossy(line).into(),
                });
            };

            if colon_pos < 1 {
                return Err(ParseError::InvalidSyntax {
                    line_number,
                    message: "Empty attribute name",
                    line: String::from_utf8_lossy(line).into(),
                });
            }

            if !in_object {
                self.callbacks.start_object();
                in_object = true;
            }

            let attr_name = &clean_line[0..colon_pos];
            let attr_value = &clean_line[colon_pos + 1..];

            if !Self::next_is_continuation(&mut reader)? {
                self.callbacks.attribute(attr_name, Self::trim(attr_value));
            } else {
                let mut accumulated = Vec::with_capacity(512);
                accumulated.extend_from_slice(Self::trim(attr_value));

                loop {
                    cont_buf.clear();
                    let Some(cont_line) = Self::read_line(&mut reader, &mut cont_buf)? else {
                        break;
                    };
                    line_number += 1;

                    if let Some(clean_cont) = Self::strip_comment(cont_line) {
                        if !clean_cont.is_empty() {
                            accumulated.push(b' ');
                            accumulated.extend_from_slice(Self::trim(&clean_cont[1..]));
                        }
                    }

                    if !Self::next_is_continuation(&mut reader)? {
                        break;
                    }
                }

                self.callbacks.attribute(attr_name, &accumulated);
            }
        }
    }

    #[inline]
    fn next_is_continuation<R: BufRead>(reader: &mut R) -> Result<bool, ParseError> {
        match Self::peek(reader)? {
            Some(ch) => Ok(Self::is_continuation(ch)),
            _ => Ok(false),
        }
    }

    #[inline]
    fn is_continuation(ch: u8) -> bool {
        matches!(ch, b'+' | b' ' | b'\t')
    }

    #[inline]
    fn trim(buf: &[u8]) -> &[u8] {
        match buf.iter().position(|&b| !b.is_ascii_whitespace()) {
            Some(n) => &buf[n..],
            None => buf,
        }
    }

    #[inline]
    fn peek<R: BufRead>(reader: &mut R) -> Result<Option<u8>, ParseError> {
        match reader.fill_buf() {
            Ok(buf) if buf.is_empty() => Ok(None),
            Ok(buf) => Ok(Some(buf[0])),
            Err(e) => Err(ParseError::Io(e)),
        }
    }

    fn read_line<'a, R: BufRead>(
        reader: &mut R,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<&'a [u8]>, ParseError> {
        match reader.read_until(b'\n', buf) {
            Ok(0) => Ok(None),
            Ok(n) if n >= 2 && buf[n - 2] == b'\r' && buf[n - 1] == b'\n' => {
                Ok(Some(&buf[0..n - 2]))
            }
            Ok(n) if n >= 1 && buf[n - 1] == b'\n' => Ok(Some(&buf[0..n - 1])),
            Ok(n) => Ok(Some(&buf[0..n])), // EOF without newline
            Err(e) => Err(ParseError::Io(e)),
        }
    }

    fn strip_comment(line: &[u8]) -> Option<&[u8]> {
        match memchr::memchr2(b'%', b'#', line) {
            None => Some(line),
            Some(0) => None,
            Some(n) => Some(&line[0..n]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::BufReader;
    use std::path::PathBuf;

    #[test]
    fn test_parse_empty() {
        let input = b"";
        let mut parser = RpslParser::new(Noop);
        parser.parse(&input[..]).unwrap();
    }

    #[test]
    fn test_parse_single_object() {
        let input = b"route: 192.0.2.0/24\norigin: AS65000\n\n";
        let mut parser = RpslParser::new(Noop);
        parser.parse(&input[..]).unwrap();
    }

    fn fixtures_dir() -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("fixtures");
        path.push("dbs");
        path
    }

    #[test]
    fn test_parse_real_world_files() {
        let dir = fixtures_dir();

        if !dir.exists() {
            eprintln!("Skipping: {} does not exist", dir.display());
            return;
        }

        let entries: Vec<_> = fs::read_dir(&dir)
            .expect("Failed to read fixtures directory")
            .filter_map(|e| e.ok())
            .filter(|e| {
                let path = e.path();
                path.is_file()
                    && (path.extension().map(|s| s == "txt").unwrap_or(false)
                        || path.extension().map(|s| s == "gz").unwrap_or(false)
                        || path.extension().map(|s| s == "db").unwrap_or(false))
            })
            .collect();

        if entries.is_empty() {
            eprintln!("No fixtures found in {}", dir.display());
            return;
        }

        for entry in entries {
            let path = entry.path();
            eprintln!("Parsing: {}", path.display());

            let file =
                fs::File::open(&path).unwrap_or_else(|_| panic!("Failed to open {:?}", path));

            let result = if path.extension().map(|s| s == "gz").unwrap_or(false) {
                let reader = BufReader::new(flate2::read::GzDecoder::new(file));
                let mut parser = RpslParser::new(Noop);
                parser.parse(reader)
            } else {
                let reader = BufReader::new(file);
                let mut parser = RpslParser::new(Noop);
                parser.parse(reader)
            };

            assert!(
                result.is_ok(),
                "Failed to parse {}: {:?}",
                path.display(),
                result.err()
            );
        }
    }
}
