use std::{
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
    collections::HashSet,
    pin::pin,
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
use loga::{
    ea,
    DebugDisplay,
    ResultContext,
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
    id_prefix: String,
    alive_task_ids: Arc<Mutex<HashSet<String>>>,
    alive: CancellationToken,
    critical: Mutex<Option<Vec<JoinHandle<Result<(), loga::Error>>>>>,
    wg: Mutex<Option<WaitGroup>>,
}

#[derive(Clone)]
pub struct TaskManager(Arc<TaskManagerInner>);

impl TaskManager {
    pub fn new() -> TaskManager {
        let tm = TaskManager(Arc::new(TaskManagerInner {
            id_prefix: "".to_string(),
            alive_task_ids: Arc::new(Mutex::new(HashSet::new())),
            alive: CancellationToken::new(),
            critical: Mutex::new(Some(Vec::new())),
            wg: Mutex::new(Some(WaitGroup::new())),
        }));
        tm.task("Signals", {
            let mut sig1 = signal(SignalKind::interrupt()).unwrap();
            let mut sig2 = signal(SignalKind::terminate()).unwrap();
            let tm = tm.clone();
            async move {
                select!{
                    _ = select(Box::pin(sig1.recv()), Box::pin(sig2.recv())) => {
                        eprintln!("Got signal, terminating.");
                        tm.terminate();
                    },
                    _ = tm.until_terminate() =>()
                }
            }
        });
        tm
    }

    fn prefix_id(&self, id: impl Into<String>) -> String {
        if self.0.id_prefix.is_empty() {
            return id.into();
        } else {
            return format!("{}/{}", self.0.id_prefix, id.into());
        }
    }

    /// Create a sub-taskmanager, which can be independently terminated but will be
    /// stopped automatically when the parent task manager is terminated.
    pub fn sub(&self, id: impl Into<String>) -> TaskManager {
        TaskManager(Arc::new(TaskManagerInner {
            id_prefix: self.prefix_id(id),
            alive_task_ids: Arc::new(Mutex::new(HashSet::new())),
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

    /// Runs a future in the background, and terminates the manager when it exits.
    /// Callers should clone the task manager and use`if_alive` for any long `.await`s
    /// within the future so that the task is aborted if shutdown is initiated.
    pub fn critical_task<T, E>(&self, id: impl Into<String>, future: T)
    where
        E: Into<loga::Error>,
        T: Future<Output = Result<(), E>> + Send + 'static {
        let id = self.prefix_id(id);
        let task_ids = self.0.alive_task_ids.clone();
        if !task_ids.lock().unwrap().insert(id.clone()) {
            panic!("Task with id {} already running!", id);
        }
        self.0.critical.lock().unwrap().as_mut().unwrap().push(spawn(async move {
            let out = future.await.context_with("Task exited with error", ea!(id = id));
            task_ids.lock().unwrap().remove(&id);
            return out;
        }));
    }

    /// Runs a future in the background. Callers should clone the task manager and
    /// use`if_alive` for any long `.await`s within the future so that the task is
    /// aborted if shutdown is initiated.
    pub fn task<T>(&self, id: impl Into<String>, future: T)
    where
        T: Future<Output = ()> + Send + 'static {
        let id = self.prefix_id(id);
        let task_ids = self.0.alive_task_ids.clone();
        if !task_ids.lock().unwrap().insert(id.clone()) {
            panic!("Task with id {} already running!", id);
        }
        let w = match self.0.wg.lock().unwrap().as_ref() {
            Some(w) => w.worker(),
            None => {
                return;
            },
        };
        spawn(async move {
            let _w = w;
            future.await;
            task_ids.lock().unwrap().remove(&id);
        });
    }

    /// Calls f with a sleep of period between invocations.  There's no concept of
    /// missed invocations.
    pub fn periodic<
        F: FnMut() -> T + Send + 'static,
        T: Future<Output = ()> + Send + 'static,
    >(&self, id: impl Into<String>, period: Duration, mut f: F) {
        let id = self.prefix_id(id);
        let task_ids = self.0.alive_task_ids.clone();
        let tm = self.clone();
        spawn(async move {
            loop {
                let _w = match tm.0.wg.lock().unwrap().as_ref() {
                    Some(w) => w.worker(),
                    None => break,
                };
                if !task_ids.lock().unwrap().insert(id.clone()) {
                    panic!("Task with id {} already running!", id);
                }
                f().await;
                drop(_w);
                task_ids.lock().unwrap().remove(&id);

                select!{
                    _ = tm.until_terminate() => {
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
    >(&self, id: impl Into<String>, mut stream: S, mut handler: Hn) {
        let id = self.prefix_id(id);
        let task_ids = self.0.alive_task_ids.clone();
        let tm = self.clone();
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
                let _w = match tm.0.wg.lock().unwrap().as_ref() {
                    Some(w) => w.worker(),
                    None => break,
                };
                if !task_ids.lock().unwrap().insert(id.clone()) {
                    panic!("Task with id {} already running!", id);
                }
                f.await;
                drop(_w);
                task_ids.lock().unwrap().remove(&id);
            }
        });
    }

    /// Initiates a graceful shutdown. Doesn't block.
    pub fn terminate(&self) {
        self.0.alive.cancel();
    }

    /// Waits for all internal managed activities to exit. Critical tasks cannot be
    /// started after this is called.  The log is used to provide context to errors
    /// while shutting down.
    pub async fn join<F: loga::Flags>(self, log: &loga::Log<F>, log_flag: F) -> Result<(), loga::Error> {
        let critical_tasks = self.0.critical.lock().unwrap().take().unwrap();
        let alive_ids = self.0.alive_task_ids.clone();
        let wg = self.0.wg.lock().unwrap().take().unwrap();
        let errs;
        if critical_tasks.is_empty() {
            self.terminate();
            let mut work = pin!(async move {
                wg.wait().await;
                return vec![];
            });
            errs = loop {
                select!{
                    errs =& mut work => break errs,
                    _ = sleep(std::time::Duration::from_secs(10)) => {
                        log.log_with(
                            log_flag,
                            "Waiting for all tasks to finish",
                            ea!(alive = (*alive_ids.lock().unwrap()).dbg_str()),
                        );
                    }
                }
            };
        } else {
            let first_critical_task_res = select_all(critical_tasks).await;
            self.terminate();
            let mut work = pin!(async move {
                let errs =
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
                wg.wait().await;
                return errs;
            });
            errs = loop {
                select!{
                    errs =& mut work => break errs,
                    _ = sleep(std::time::Duration::from_secs(10)) => {
                        log.log_with(
                            log_flag,
                            "Waiting for all tasks to finish",
                            ea!(alive = (*alive_ids.lock().unwrap()).dbg_str()),
                        );
                    }
                }
            };
        }
        if !errs.is_empty() {
            return Err(loga::agg_err("The task manager exited after critical tasks failed", errs));
        }
        return Ok(())
    }
}
