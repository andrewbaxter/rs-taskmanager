use std::{
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};
use futures::{
    Future,
    future::{
        select,
        join_all,
    },
    StreamExt,
};
use loga::Log;
use tokio::{
    select,
    signal::{
        unix::{
            signal,
            SignalKind,
        },
    },
    spawn,
    sync::Notify,
    time::sleep,
    task::JoinHandle,
};
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
    critical: Mutex<Option<Vec<JoinHandle<Result<(), loga::Error>>>>>,
    wg: Mutex<Option<WaitGroup>>,
}

#[derive(Clone)]
pub struct TaskManager(Arc<TaskManagerInner>);

impl TaskManager {
    pub fn new() -> TaskManager {
        let tm = TaskManager(Arc::new(TaskManagerInner {
            alive: Permanotify::new(),
            critical: Mutex::new(Some(Vec::new())),
            wg: Mutex::new(Some(WaitGroup::new())),
        }));
        let tm1 = tm.clone();
        spawn(async move {
            let mut sig1 = signal(SignalKind::interrupt()).unwrap();
            let mut sig2 = signal(SignalKind::terminate()).unwrap();
            match tm1.if_alive(select(Box::pin(sig1.recv()), Box::pin(sig2.recv()))).await {
                Some(_) => {
                    println!("Got signal, terminating.");
                    tm1.terminate();
                },
                None => { },
            };
        });
        tm
    }

    /// Create a sub-taskmanager, which can be independently terminated but will be
    /// stopped automatically when the parent task manager is terminated.  The log is
    /// used to provide context to the the child's join within the parent's management.
    pub fn sub(&self, log: Log) -> TaskManager {
        let tm = Self::new();
        let tm_child = tm.clone();
        let tm_parent = self.clone();
        self.critical_task(async move {
            select!{
                _ = tm_child.until_terminate() => {
                },
                _ = tm_parent.until_terminate() => {
                    tm_child.terminate();
                    tm_child.join(log).await?;
                },
            };

            let r: Result<(), loga::Error> = Ok(());
            r
        });
        tm
    }

    /// Wait until graceful shutdown is initiated (doesn't wait for all tasks to
    /// finish; use join for that).
    pub async fn until_terminate(&self) {
        self.0.alive.wait().await;
    }

    /// Wraps a future and cancels it if a graceful shutdown is initiated. If the
    /// cancel occurs, returns None, otherwise Some(original future return).
    pub async fn if_alive<O, T: Future<Output = O> + Send>(&self, future: T) -> Option<O> {
        let n = self.0.alive.wait();

        select!{
            _ = n => None,
            v = future => Some(v),
        }
    }

    /// Runs a future in the background, and terminates the manager when it exits.
    /// Callers should clone the task manager and use`if_alive` for any long `.await`s
    /// within the future so that the task is aborted if shutdown is initiated.
    pub fn critical_task<T, E>(&self, future: T)
    where
        E: Into<loga::Error>,
        T: Future<Output = Result<(), E>> + Send + 'static {
        self.0.critical.lock().unwrap().as_mut().unwrap().push(spawn(async move {
            future.await.map_err(|e| loga::errors("Critical task exited with error", vec![e.into()]))
        }));
    }

    /// Runs a future in the background. Callers should clone the task manager and
    /// use`if_alive` for any long `.await`s within the future so that the task is
    /// aborted if shutdown is initiated.
    pub fn task<T>(&self, future: T)
    where
        T: Future<Output = ()> + Send + 'static {
        let w = match self.0.wg.lock().unwrap().as_ref() {
            Some(w) => w.worker(),
            None => {
                return;
            },
        };
        spawn(async move {
            let _w = w;
            future.await;
        });
    }

    /// Calls f with a sleep of period between invocations.  There's no concept of
    /// missed invocations.
    pub fn periodic<
        F: FnMut() -> T + Send + 'static,
        T: Future<Output = ()> + Send + 'static,
    >(&self, period: Duration, mut f: F) {
        let tm0 = self.clone();
        let manager = self.clone();
        spawn(async move {
            loop {
                let _w = match tm0.0.wg.lock().unwrap().as_ref() {
                    Some(w) => w.worker(),
                    None => break,
                };
                f().await;
                drop(_w);
                let n = manager.0.alive.wait();

                select!{
                    _ = n => {
                        break;
                    }
                    _ = sleep(period) => {
                    }
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
    >(&self, mut stream: S, mut handler: Hn) {
        let tm0 = self.clone();
        let manager = self.clone();
        spawn(async move {
            loop {
                // Convoluted to work around poor rust lifetime analysis
                // https://github.com/rust-lang/rust/issues/63768
                let n = manager.0.alive.wait();
                let f = {
                    let e = select!{
                        _ = n => {
                            break;
                        }
                        e = stream.next() => {
                            e
                        }
                    };
                    match e {
                        Some(x) => handler(x),
                        None => break,
                    }
                };
                let _w = match tm0.0.wg.lock().unwrap().as_ref() {
                    Some(w) => w.worker(),
                    None => break,
                };
                f.await;
                drop(_w);
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

    /// Waits for all internal managed activities to exit. Critical tasks cannot be
    /// started after this is called.  The log is used to provide context to errors
    /// while shutting down.
    pub async fn join(self, log: Log) -> Result<(), loga::Error> {
        let critical_tasks = self.0.critical.lock().unwrap().take().unwrap();
        let errs = join_all(critical_tasks).await.into_iter().filter_map(|r| match r {
            Ok(r) => match r {
                Ok(_) => None,
                Err(e) => Some(e),
            },
            Err(e) => Some(loga::err!(log, "Critical task panicked", err = e)),
        }).collect::<Vec<loga::Error>>();
        loga::log_debug!(log, "Join, done waiting on critical tasks");
        let wg = self.0.wg.lock().unwrap().take().unwrap();
        loga::log_debug!(log, "Join, done waiting on wait group");
        wg.wait().await;
        if !errs.is_empty() {
            return Err(loga::errors("The task manager exited after critical tasks failed", errs));
        }
        loga::log_debug!(log, "Join, done");
        return Ok(())
    }
}
