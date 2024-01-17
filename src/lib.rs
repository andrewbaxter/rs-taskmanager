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
        select_all,
    },
    StreamExt,
};
use tokio::{
    select,
    signal::{
        unix::{
            signal,
            SignalKind,
        },
    },
    spawn,
    time::sleep,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use waitgroup::WaitGroup;

struct TaskManagerInner {
    alive: CancellationToken,
    critical: Mutex<Option<Vec<JoinHandle<Result<(), loga::Error>>>>>,
    wg: Mutex<Option<WaitGroup>>,
}

#[derive(Clone)]
pub struct TaskManager(Arc<TaskManagerInner>);

impl TaskManager {
    pub fn new() -> TaskManager {
        let tm = TaskManager(Arc::new(TaskManagerInner {
            alive: CancellationToken::new(),
            critical: Mutex::new(Some(Vec::new())),
            wg: Mutex::new(Some(WaitGroup::new())),
        }));
        let tm1 = tm.clone();
        let mut sig1 = signal(SignalKind::interrupt()).unwrap();
        let mut sig2 = signal(SignalKind::terminate()).unwrap();
        tm.task(async move {
            match tm1.if_alive(select(Box::pin(sig1.recv()), Box::pin(sig2.recv()))).await {
                Some(_) => {
                    eprintln!("Got signal, terminating.");
                    tm1.terminate();
                },
                None => { },
            };
        });
        tm
    }

    /// Create a sub-taskmanager, which can be independently terminated but will be
    /// stopped automatically when the parent task manager is terminated.
    pub fn sub(&self) -> TaskManager {
        TaskManager(Arc::new(TaskManagerInner {
            alive: self.0.alive.child_token(),
            critical: Mutex::new(Some(Vec::new())),
            wg: Mutex::new(Some(WaitGroup::new())),
        }))
    }

    /// Wait until graceful shutdown is initiated (doesn't wait for all tasks to
    /// finish; use join for that).
    pub async fn until_terminate(&self) {
        self.0.alive.cancelled().await;
    }

    /// Wraps a future and cancels it if a graceful shutdown is initiated. If the
    /// cancel occurs, returns None, otherwise Some(original future return).
    pub async fn if_alive<O, T: Future<Output = O> + Send>(&self, future: T) -> Option<O> {
        let n = self.0.alive.cancelled();

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
            future.await.map_err(|e| e.into())
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
                let n = manager.0.alive.cancelled();

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
                let n = manager.0.alive.cancelled();
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
        self.0.alive.cancel();
    }

    /// Check if the task manager has requested shutdown.
    pub fn alive(&self) -> bool {
        return self.0.alive.is_cancelled();
    }

    /// Waits for all internal managed activities to exit. Critical tasks cannot be
    /// started after this is called.  The log is used to provide context to errors
    /// while shutting down.
    pub async fn join(self) -> Result<(), loga::Error> {
        let critical_tasks = self.0.critical.lock().unwrap().take().unwrap();
        let errs;
        if critical_tasks.is_empty() {
            self.terminate();
            errs = vec![];
        } else {
            let first_critical_task_res = select_all(critical_tasks).await;
            self.terminate();
            errs =
                vec![first_critical_task_res.0]
                    .into_iter()
                    .chain(join_all(first_critical_task_res.2).await.into_iter())
                    .filter_map(|r| match r {
                        Ok(r) => match r {
                            Ok(_) => None,
                            Err(e) => Some(e),
                        },
                        Err(e) => Some(e.into()),
                    })
                    .collect::<Vec<loga::Error>>();
        }
        let wg = self.0.wg.lock().unwrap().take().unwrap();
        wg.wait().await;
        if !errs.is_empty() {
            return Err(loga::agg_err("The task manager exited after critical tasks failed", errs));
        }
        return Ok(())
    }
}
