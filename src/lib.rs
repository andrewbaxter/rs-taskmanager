use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{Future, StreamExt};
use tokio::{io, select, signal, spawn, sync::Notify, time::sleep};
use waitgroup::WaitGroup;

struct Permanotify_ {
    fired: bool,
    notify: Arc<Notify>,
}

struct Permanotify(Mutex<Permanotify_>);

impl Permanotify {
    fn new() -> Permanotify {
        Permanotify(Mutex::new(Permanotify_ {
            fired: false,
            notify: Arc::new(Notify::new()),
        }))
    }

    async fn wait(&self) {
        let p = {
            let p = self.0.lock().unwrap();
            if p.fired {
                return;
            }
            p.notify.clone()
        };
        p.notified().await
    }

    fn yes(&self) -> bool {
        return !self.0.lock().unwrap().fired;
    }

    fn no(&self) {
        let mut p = self.0.lock().unwrap();
        p.fired = true;
        p.notify.notify_waiters();
    }
}

struct TaskManagerInner {
    alive: Permanotify,
    wg: Mutex<Option<WaitGroup>>,
}

#[derive(Clone)]
pub struct TaskManager(Arc<TaskManagerInner>);

impl TaskManager {
    pub fn new() -> TaskManager {
        let tm = TaskManager(Arc::new(TaskManagerInner {
            alive: Permanotify::new(),
            wg: Mutex::new(Some(WaitGroup::new())),
        }));
        tm
    }

    /// Attaches to the SIGINT signal, to call terminate when triggered.
    pub async fn attach_sigint<L: FnOnce(io::Error) + Send + 'static>(
        &self,
        signal_error_logger: L,
    ) {
        let tm = self.clone();
        spawn(async move {
            match tm.if_alive(signal::ctrl_c()).await {
                Some(r) => match r {
                    Ok(_) => {
                        tm.terminate();
                    }
                    Err(e) => {
                        signal_error_logger(e);
                    }
                },
                None => {}
            };
        });
    }

    /// Wraps a future and cancels it if a graceful shutdown is initiated. If the cancel occurs,
    /// returns None, otherwise Some(original future return).
    pub async fn if_alive<O, T: Future<Output = O> + Send>(&self, future: T) -> Option<O> {
        let n = self.0.alive.wait();
        select! {
            _ = n => None,
            v = future => Some(v),
        }
    }

    /// Runs a future in the background. Callers should clone the task manager and use `if_alive` for any
    /// long `.await`s within the future so that the task is aborted if shutdown is initiated.
    pub fn task<T>(&self, future: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        let w = self.0.wg.lock().unwrap().as_ref().unwrap().worker();
        spawn(async move {
            let _w = w;
            future.await;
        });
    }

    /// Calls f with a sleep of period between invocations.  There's no concept of missed invocations.
    pub fn periodic<F: FnMut() -> T + Send + 'static, T: Future<Output = ()> + Send + 'static>(
        &self,
        period: Duration,
        mut f: F,
    ) {
        let w = self.0.wg.lock().unwrap().as_ref().unwrap().worker();
        let manager = self.clone();
        spawn(async move {
            let _w = w;
            loop {
                f().await;
                let n = manager.0.alive.wait();
                select! {
                    _ = n => { break; }
                    _ = sleep(period) => {}
                };
            }
        });
    }

    /// Calls handler for each element in stream, until the stream ends or the graceful
    /// shutdown is initiated.
    pub fn stream<
        T,
        S: StreamExt<Item = T> + Send + 'static + Unpin,
        Hn: FnMut(T) -> F + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    >(
        &self,
        mut stream: S,
        mut handler: Hn,
    ) {
        let w = self.0.wg.lock().unwrap().as_ref().unwrap().worker();
        let manager = self.clone();
        spawn(async move {
            let _w = w;
            loop {
                // Convoluted to work around poor rust lifetime analysis
                // https://github.com/rust-lang/rust/issues/63768
                let n = manager.0.alive.wait();
                let f = {
                    let e = select! {
                        _ = n => { break; }
                        e = stream.next() => {e }
                    };
                    match e {
                        Some(x) => handler(x),
                        None => break,
                    }
                };
                f.await;
            }
        });
    }

    /// Initiates a graceful shutdown. Doesn't block.
    pub fn terminate(&self) {
        self.0.alive.no();
    }

    /// Check if the task manager has requested shutdown.
    pub fn alive(&self) -> bool {
        return self.0.alive.yes();
    }

    /// Waits for all internal managed activities to exit.
    pub async fn join(self) {
        let wg = self.0.wg.lock().unwrap().take().unwrap();
        wg.wait().await;
    }
}
