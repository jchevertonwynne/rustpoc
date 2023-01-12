use std::future::Future;
use std::sync::Arc;
use tokio::sync::Notify;

pub mod db;
pub mod grpc;
pub mod rabbit;
pub mod server;

pub struct Killer {
    inner: Arc<Notify>,
}

impl Default for Killer {
    fn default() -> Self {
        Killer::new()
    }
}

impl Killer {
    pub fn new() -> Killer {
        Killer {
            inner: Arc::new(Notify::new()),
        }
    }

    pub fn kill_signal(&self) -> impl Future<Output = ()> + Send + 'static {
        let notify = Arc::clone(&self.inner);
        async move { notify.notified().await }
    }

    pub fn kill(self) {
        self.inner.notify_waiters()
    }
}
