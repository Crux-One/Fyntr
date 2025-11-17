use std::sync::Arc;

use tokio::{
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::Mutex,
};

/// Helper to obtain an `OwnedWriteHalf` backed by a live TCP connection for tests.
pub(crate) async fn make_backend_write() -> Arc<Mutex<OwnedWriteHalf>> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let accept_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        stream
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let server_stream = accept_handle.await.unwrap();
    drop(client_stream);

    let (_read_half, write_half) = server_stream.into_split();
    Arc::new(Mutex::new(write_half))
}
