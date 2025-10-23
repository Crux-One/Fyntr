use anyhow::{Result, anyhow};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

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
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(anyhow!("Invalid request line: {}", line));
        }

        Ok(RequestLine {
            method: parts[0].to_string(),
            target: parts[1].to_string(),
            version: parts[2].to_string(),
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
    pub(crate) fn parse_connect_target(&self) -> Result<(String, u16)> {
        if !self.is_connect_method() {
            return Err(anyhow!("Not a CONNECT request"));
        }

        let parts: Vec<&str> = self.target.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid CONNECT target: {}", self.target));
        }

        let host = parts[0].to_string();
        let port = parts[1]
            .parse::<u16>()
            .map_err(|_| anyhow!("Invalid port: {}", parts[1]))?;

        Ok((host, port))
    }
}

/// Reads the first line (request line) of an HTTP request from the given BufReader.
pub(crate) async fn read_request_line(
    reader: &mut BufReader<OwnedReadHalf>,
) -> Result<RequestLine> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    if line.is_empty() {
        return Err(anyhow!("Empty request line"));
    }

    let line = line.trim();
    RequestLine::parse(line)
}

/// Skips all headers of a CONNECT request until a blank line is encountered (end of headers).
pub(crate) async fn skip_headers(reader: &mut BufReader<OwnedReadHalf>) -> Result<()> {
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;

        if line.trim().is_empty() {
            break;
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
        let req = RequestLine::parse("CONNECT example.com HTTP/1.1").unwrap();
        let err = req.parse_connect_target().unwrap_err();
        assert!(err.to_string().contains("Invalid CONNECT target"));
    }

    #[test]
    fn test_parse_connect_target_rejects_bad_port() {
        let req = RequestLine::parse("CONNECT example.com:not-a-port HTTP/1.1").unwrap();
        let err = req.parse_connect_target().unwrap_err();
        assert!(err.to_string().contains("Invalid port"));
    }

    #[test]
    fn test_is_connect_method() {
        let req = RequestLine::parse("CONNECT example.com:443 HTTP/1.1").unwrap();
        assert!(req.is_connect_method());

        let req = RequestLine::parse("GET /api HTTP/1.1").unwrap();
        assert!(!req.is_connect_method());
    }

    #[test]
    fn test_is_http_1x() {
        let req = RequestLine::parse("CONNECT example.com:443 HTTP/1.1").unwrap();
        assert!(req.is_http_1x());

        let req = RequestLine::parse("CONNECT example.com:443 HTTP/1.0").unwrap();
        assert!(req.is_http_1x());

        let req = RequestLine::parse("CONNECT example.com:443 HTTP/2.0").unwrap();
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
            .write_all(b"CONNECT example.com:443 HTTP/1.1\r\n")
            .await
            .unwrap();

        let line = read_request_line(&mut reader).await.unwrap();
        assert_eq!(line.method, "CONNECT");
        assert_eq!(line.target, "example.com:443");
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
    async fn test_skip_headers_consumes_until_blank_line() {
        let (mut reader, _write_half, mut client_stream) = build_loopback_pair().await;

        client_stream
            .write_all(
                b"CONNECT example.com:443 HTTP/1.1\r\nHeader: value\r\nAnother: header\r\n\r\nBody-Line\r\n",
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
    async fn test_send_connect_response_writes_expected_bytes() {
        let (_reader, mut write_half, mut client_stream) = build_loopback_pair().await;

        send_connect_response(&mut write_half).await.unwrap();

        let expected = b"HTTP/1.1 200 Connection Established\r\n\r\n";
        let mut buf = vec![0u8; expected.len()];
        client_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(expected, &buf[..]);
    }
}
