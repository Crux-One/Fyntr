use std::fmt;

use log::{error, warn};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

use crate::{flow::FlowId, http::request::RequestReadError};

use super::{CONNECT_LOG_TARGET, ConnectFlowError, ConnectResult};

/// Prebuilt HTTP status line used when responding to failed CONNECT requests.
/// Holds the raw bytes sent to the client plus the parsed pieces for logging.
#[derive(Clone, Copy)]
pub(super) struct StatusLine {
    raw: &'static [u8],
    code: &'static str,
    reason: &'static str,
}

impl StatusLine {
    const fn new(raw: &'static [u8], code: &'static str, reason: &'static str) -> Self {
        Self { raw, code, reason }
    }

    pub(super) const METHOD_NOT_ALLOWED: StatusLine = Self::new(
        b"HTTP/1.1 405 Method Not Allowed\r\n\r\n",
        "405",
        "Method Not Allowed",
    );
    pub(super) const VERSION_NOT_SUPPORTED: StatusLine = Self::new(
        b"HTTP/1.1 505 HTTP Version Not Supported\r\n\r\n",
        "505",
        "HTTP Version Not Supported",
    );
    pub(super) const BAD_GATEWAY: StatusLine =
        Self::new(b"HTTP/1.1 502 Bad Gateway\r\n\r\n", "502", "Bad Gateway");
    pub(super) const FORBIDDEN: StatusLine =
        Self::new(b"HTTP/1.1 403 Forbidden\r\n\r\n", "403", "Forbidden");
    pub(super) const REQUEST_TIMEOUT: StatusLine = Self::new(
        b"HTTP/1.1 408 Request Timeout\r\n\r\n",
        "408",
        "Request Timeout",
    );
    pub(super) const BAD_REQUEST: StatusLine =
        Self::new(b"HTTP/1.1 400 Bad Request\r\n\r\n", "400", "Bad Request");
    pub(super) const URI_TOO_LONG: StatusLine =
        Self::new(b"HTTP/1.1 414 URI Too Long\r\n\r\n", "414", "URI Too Long");
    pub(super) const HEADER_FIELDS_TOO_LARGE: StatusLine = Self::new(
        b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n",
        "431",
        "Request Header Fields Too Large",
    );
    pub(super) const SERVICE_UNAVAILABLE: StatusLine = Self::new(
        b"HTTP/1.1 503 Service Unavailable\r\n\r\n",
        "503",
        "Service Unavailable",
    );
}

#[derive(Clone, Copy)]
pub(super) enum StatusLogLevel {
    Warn,
    Error,
}

pub(super) async fn respond_with_status<T>(
    flow_id: FlowId,
    writer: &mut OwnedWriteHalf,
    status: StatusLine,
    level: StatusLogLevel,
    detail: impl fmt::Display,
) -> ConnectResult<T> {
    match level {
        StatusLogLevel::Warn => warn!(
            target: CONNECT_LOG_TARGET,
            "flow{}: {} ({} {})",
            flow_id.0, detail, status.code, status.reason
        ),
        StatusLogLevel::Error => error!(
            target: CONNECT_LOG_TARGET,
            "flow{}: {} ({} {})",
            flow_id.0, detail, status.code, status.reason
        ),
    }

    writer.write_all(status.raw).await?;
    writer.flush().await?;
    Err(ConnectFlowError::ResponseSent)
}

pub(super) fn classify_input_error(err: &anyhow::Error) -> StatusLine {
    if let Some(read_err) = err.downcast_ref::<RequestReadError>() {
        match read_err {
            RequestReadError::RequestLineTooLong { .. } => StatusLine::URI_TOO_LONG,
            RequestReadError::HeaderLineTooLong { .. }
            | RequestReadError::HeadersTooLarge { .. }
            | RequestReadError::TooManyHeaders { .. } => StatusLine::HEADER_FIELDS_TOO_LARGE,
            RequestReadError::InvalidEncoding => StatusLine::BAD_REQUEST,
        }
    } else {
        StatusLine::BAD_REQUEST
    }
}
