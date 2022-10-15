use futures_lite::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::broadcast::Receiver;

pub mod db;
pub mod grpc;
pub mod rabbit;
pub mod server;

pub struct Holder {
    inner: Receiver<()>,
}

impl Holder {
    fn new(inner: Receiver<()>) -> Holder {
        Holder { inner }
    }
}

impl Future for Holder {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Box::pin(self.inner.recv()).poll(cx).map(|val| {
            if let Err(err) = val {
                tracing::error!("failed to recv shutdown: {:?}", err);
            }
        })
    }
}
