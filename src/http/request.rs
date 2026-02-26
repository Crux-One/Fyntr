use std::{error::Error, fmt};

use anyhow::{Result, anyhow};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::limits::{
    MAX_HEADER_BYTES, MAX_HEADER_LINE_BYTES, MAX_HEADER_LINES, MAX_REQUEST_LINE_BYTES,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestReadError {
    RequestLineTooLong { max_bytes: usize },
    HeaderLineTooLong { max_bytes: usize },
    HeadersTooLarge { max_bytes: usize },
    TooManyHeaders { max_headers: usize },
    InvalidEncoding,
}

impl fmt::Display for RequestReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequestLineTooLong { max_bytes } => {
                write!(f, "request line exceeds {} bytes", max_bytes)
            }
            Self::HeaderLineTooLong { max_bytes } => {
                write!(f, "header line exceeds {} bytes", max_bytes)
            }
            Self::HeadersTooLarge { max_bytes } => {
                write!(f, "headers exceed {} bytes", max_bytes)
            }
            Self::TooManyHeaders { max_headers } => {
                write!(f, "header count exceeds {}", max_headers)
            }
            Self::InvalidEncoding => f.write_str("request contains invalid UTF-8"),
        }
    }
}

impl Error for RequestReadError {}

fn decode_line(buf: &[u8]) -> Result<String> {
    std::str::from_utf8(buf)
        .map(|s| s.to_string())
        .map_err(|_| RequestReadError::InvalidEncoding.into())
}

async fn read_line_with_limit(
    reader: &mut BufReader<OwnedReadHalf>,
    max_bytes: usize,
    limit_err: RequestReadError,
) -> Result<Vec<u8>> {
    let mut line = Vec::new();

    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            return Ok(line);
        }

        let (chunk_len, has_newline) = match buf.iter().position(|&b| b == b'\n') {
            Some(pos) => (pos + 1, true),
            None => (buf.len(), false),
        };

        if line.len().saturating_add(chunk_len) > max_bytes {
            return Err(limit_err.into());
        }

        line.extend_from_slice(&buf[..chunk_len]);
        reader.consume(chunk_len);

        if has_newline {
            return Ok(line);
        }
    }
}

/// The first line of an HTTP request (request line), e.g., "CONNECT api.aws.amazon.com:443 HTTP/1.1"
#[derive(Debug)]
pub(crate) struct RequestLine {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) version: String,
}

impl RequestLine {
    /// Parse the request line into method, target, and version fields.
    /// Example: "CONNECT api.aws.amazon.com:443 HTTP/1.1"
    pub(crate) fn parse(line: &str) -> Result<Self> {
        let mut parts = line.split_whitespace();
        let (method, target, version) =
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some(method), Some(target), Some(version), None) => (method, target, version),
                _ => return Err(anyhow!("Invalid request line")),
            };

        Ok(RequestLine {
            method: method.to_string(),
            target: target.to_string(),
            version: version.to_string(),
        })
    }

    /// Returns true if the HTTP method is CONNECT (case-insensitive).
    pub(crate) fn is_connect_method(&self) -> bool {
        self.method.eq_ignore_ascii_case("CONNECT")
    }

    /// Returns true if the HTTP version is HTTP/1.x (e.g., HTTP/1.0 or HTTP/1.1).
    pub(crate) fn is_http_1x(&self) -> bool {
        self.version.starts_with("HTTP/1.")
    }

    /// Extracts the host and port from the CONNECT target.
    /// Example: "api.aws.amazon.com:443" -> ("api.aws.amazon.com", 443)
    /// Note: IPv6 bracket form (e.g., "[2001:db8::1]:443") is not supported.
    pub(crate) fn parse_connect_target(&self) -> Result<(String, u16)> {
        if !self.is_connect_method() {
            return Err(anyhow!("Not a CONNECT request"));
        }

        let (host, port) = self
            .target
            .split_once(':')
            .ok_or_else(|| anyhow!("Invalid CONNECT target"))?;
        let host = host.to_string();
        let port = port.parse::<u16>().map_err(|_| anyhow!("Invalid port"))?;

        Ok((host, port))
    }
}

/// Reads the first line (request line) of an HTTP request from the given BufReader.
pub(crate) async fn read_request_line(
    reader: &mut BufReader<OwnedReadHalf>,
) -> Result<RequestLine> {
    let line = read_line_with_limit(
        reader,
        MAX_REQUEST_LINE_BYTES,
        RequestReadError::RequestLineTooLong {
            max_bytes: MAX_REQUEST_LINE_BYTES,
        },
    )
    .await?;

    if line.is_empty() {
        return Err(anyhow!("Empty request line"));
    }

    let line = decode_line(&line)?;
    let line = line.trim();
    RequestLine::parse(line)
}

/// Skips all headers of a CONNECT request until a blank line is encountered (end of headers).
pub(crate) async fn skip_headers(reader: &mut BufReader<OwnedReadHalf>) -> Result<()> {
    let mut total_bytes = 0usize;
    let mut header_lines = 0usize;

    loop {
        let line = read_line_with_limit(
            reader,
            MAX_HEADER_LINE_BYTES,
            RequestReadError::HeaderLineTooLong {
                max_bytes: MAX_HEADER_LINE_BYTES,
            },
        )
        .await?;

        if line.is_empty() {
            return Err(anyhow!("Unexpected EOF while reading headers"));
        }

        total_bytes = total_bytes.saturating_add(line.len());
        if total_bytes > MAX_HEADER_BYTES {
            return Err(RequestReadError::HeadersTooLarge {
                max_bytes: MAX_HEADER_BYTES,
            }
            .into());
        }

        let line = decode_line(&line)?;
        if line.trim().is_empty() {
            break;
        }

        header_lines = header_lines.saturating_add(1);
        if header_lines > MAX_HEADER_LINES {
            return Err(RequestReadError::TooManyHeaders {
                max_headers: MAX_HEADER_LINES,
            }
            .into());
        }
    }
    Ok(())
}

/// Sends a "200 Connection Established" response to the client after a successful CONNECT handshake.
pub(crate) async fn send_connect_response(writer: &mut OwnedWriteHalf) -> Result<()> {
    let response = b"HTTP/1.1 200 Connection Established\r\n\r\n";
    writer.write_all(response).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    #[test]
    fn test_parse_request_line() {
        let line = "CONNECT api.aws.amazon.com:443 HTTP/1.1";
        let req = RequestLine::parse(line).unwrap();
        assert_eq!(req.method, "CONNECT");
        assert_eq!(req.target, "api.aws.amazon.com:443");
        assert_eq!(req.version, "HTTP/1.1");
    }

    #[test]
    fn test_parse_request_line_rejects_short_lines() {
        let err = RequestLine::parse("CONNECT only-two").unwrap_err();
        assert!(err.to_string().contains("Invalid request line"));
        assert!(!err.to_string().contains("only-two"));
    }

    #[test]
    fn test_parse_request_line_rejects_too_many_parts() {
        let err = RequestLine::parse("CONNECT host:443 HTTP/1.1 extra").unwrap_err();
        assert!(err.to_string().contains("Invalid request line"));
        assert!(!err.to_string().contains("extra"));
    }

    #[test]
    fn test_parse_connect_target() {
        let line = "CONNECT api.aws.amazon.com:443 HTTP/1.1";
        let req = RequestLine::parse(line).unwrap();
        let (host, port) = req.parse_connect_target().unwrap();
        assert_eq!(host, "api.aws.amazon.com");
        assert_eq!(port, 443);
    }

    #[test]
    fn test_parse_connect_target_non_connect() {
        let req = RequestLine::parse("GET / HTTP/1.1").unwrap();
        let err = req.parse_connect_target().unwrap_err();
        assert!(err.to_string().contains("Not a CONNECT"));
    }

    #[test]
    fn test_parse_connect_target_rejects_missing_port() {
        let req = RequestLine::parse("CONNECT example.invalid HTTP/1.1").unwrap();
        let err = req.parse_connect_target().unwrap_err();
        assert!(err.to_string().contains("Invalid CONNECT target"));
        assert!(!err.to_string().contains("example.invalid"));
    }

    #[test]
    fn test_parse_connect_target_rejects_bad_port() {
        let req = RequestLine::parse("CONNECT example.invalid:not-a-port HTTP/1.1").unwrap();
        let err = req.parse_connect_target().unwrap_err();
        assert!(err.to_string().contains("Invalid port"));
        assert!(!err.to_string().contains("not-a-port"));
    }

    #[test]
    fn test_is_connect_method() {
        let req = RequestLine::parse("CONNECT example.invalid:443 HTTP/1.1").unwrap();
        assert!(req.is_connect_method());

        let req = RequestLine::parse("GET /api HTTP/1.1").unwrap();
        assert!(!req.is_connect_method());
    }

    #[test]
    fn test_is_http_1x() {
        let req = RequestLine::parse("CONNECT example.invalid:443 HTTP/1.1").unwrap();
        assert!(req.is_http_1x());

        let req = RequestLine::parse("CONNECT example.invalid:443 HTTP/1.0").unwrap();
        assert!(req.is_http_1x());

        let req = RequestLine::parse("CONNECT example.invalid:443 HTTP/2.0").unwrap();
        assert!(!req.is_http_1x());
    }

    async fn build_loopback_pair() -> (BufReader<OwnedReadHalf>, OwnedWriteHalf, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let server = accept_handle.await.unwrap();
        let (server_read, server_write) = server.into_split();

        (BufReader::new(server_read), server_write, client)
    }

    #[tokio::test]
    async fn test_read_request_line_success() {
        let (mut reader, _server_write, mut client_stream) = build_loopback_pair().await;

        client_stream
            .write_all(b"CONNECT example.invalid:443 HTTP/1.1\r\n")
            .await
            .unwrap();

        let line = read_request_line(&mut reader).await.unwrap();
        assert_eq!(line.method, "CONNECT");
        assert_eq!(line.target, "example.invalid:443");
        assert_eq!(line.version, "HTTP/1.1");
    }

    #[tokio::test]
    async fn test_read_request_line_empty() {
        let (mut reader, _server_write, client_stream) = build_loopback_pair().await;

        drop(client_stream);

        let err = read_request_line(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("Empty request line"));
    }

    #[tokio::test]
    async fn test_read_request_line_rejects_too_long_line() {
        let (mut reader, _server_write, mut client_stream) = build_loopback_pair().await;
        let long_target = "a".repeat(MAX_REQUEST_LINE_BYTES);
        let request = format!("CONNECT {}:443 HTTP/1.1\r\n", long_target);
        client_stream.write_all(request.as_bytes()).await.unwrap();

        let err = read_request_line(&mut reader).await.unwrap_err();
        let typed = err.downcast_ref::<RequestReadError>().unwrap();
        assert_eq!(
            typed,
            &RequestReadError::RequestLineTooLong {
                max_bytes: MAX_REQUEST_LINE_BYTES
            }
        );
    }

    #[tokio::test]
    async fn test_skip_headers_consumes_until_blank_line() {
        let (mut reader, _write_half, mut client_stream) = build_loopback_pair().await;

        client_stream
            .write_all(
                b"CONNECT example.invalid:443 HTTP/1.1\r\nHeader: value\r\nAnother: header\r\n\r\nBody-Line\r\n",
            )
            .await
            .unwrap();

        let _ = read_request_line(&mut reader).await.unwrap();
        skip_headers(&mut reader).await.unwrap();

        let mut next_line = String::new();
        reader.read_line(&mut next_line).await.unwrap();
        assert_eq!(next_line, "Body-Line\r\n");
    }

    #[tokio::test]
    async fn test_skip_headers_rejects_too_many_headers() {
        let (mut reader, _write_half, mut client_stream) = build_loopback_pair().await;
        let mut request = String::from("CONNECT example.invalid:443 HTTP/1.1\r\n");
        for i in 0..=MAX_HEADER_LINES {
            request.push_str(&format!("X-Test-{}: value\r\n", i));
        }
        request.push_str("\r\n");
        client_stream.write_all(request.as_bytes()).await.unwrap();

        let _ = read_request_line(&mut reader).await.unwrap();
        let err = skip_headers(&mut reader).await.unwrap_err();
        let typed = err.downcast_ref::<RequestReadError>().unwrap();
        assert_eq!(
            typed,
            &RequestReadError::TooManyHeaders {
                max_headers: MAX_HEADER_LINES
            }
        );
    }

    #[tokio::test]
    async fn test_skip_headers_rejects_large_header_line() {
        let (mut reader, _write_half, mut client_stream) = build_loopback_pair().await;
        let oversized_value = "b".repeat(MAX_HEADER_LINE_BYTES);
        let request = format!(
            "CONNECT example.invalid:443 HTTP/1.1\r\nX-Long: {}\r\n\r\n",
            oversized_value
        );
        client_stream.write_all(request.as_bytes()).await.unwrap();

        let _ = read_request_line(&mut reader).await.unwrap();
        let err = skip_headers(&mut reader).await.unwrap_err();
        let typed = err.downcast_ref::<RequestReadError>().unwrap();
        assert_eq!(
            typed,
            &RequestReadError::HeaderLineTooLong {
                max_bytes: MAX_HEADER_LINE_BYTES
            }
        );
    }

    #[tokio::test]
    async fn test_skip_headers_rejects_headers_too_large_total_bytes() {
        let (mut reader, _write_half, mut client_stream) = build_loopback_pair().await;
        let mut request = String::from("CONNECT example.invalid:443 HTTP/1.1\r\n");
        let value = "c".repeat(4000);
        for i in 0..9 {
            request.push_str(&format!("X-Bulk-{}: {}\r\n", i, value));
        }
        request.push_str("\r\n");
        client_stream.write_all(request.as_bytes()).await.unwrap();

        let _ = read_request_line(&mut reader).await.unwrap();
        let err = skip_headers(&mut reader).await.unwrap_err();
        let typed = err.downcast_ref::<RequestReadError>().unwrap();
        assert_eq!(
            typed,
            &RequestReadError::HeadersTooLarge {
                max_bytes: MAX_HEADER_BYTES
            }
        );
    }

    #[tokio::test]
    async fn test_send_connect_response_writes_expected_bytes() {
        let (_reader, mut write_half, mut client_stream) = build_loopback_pair().await;

        send_connect_response(&mut write_half).await.unwrap();

        let expected = b"HTTP/1.1 200 Connection Established\r\n\r\n";
        let mut buf = vec![0u8; expected.len()];
        client_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(expected, &buf[..]);
    }
}
