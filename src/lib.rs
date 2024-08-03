use {
    futures::{
        future::{
            join_all,
            select,
            select_all,
        },
        Future,
        StreamExt,
    },
    loga::{
        ea,
        DebugDisplay,
        ResultContext,
    },
    std::{
        collections::HashSet,
        pin::pin,
        sync::{
            Arc,
            Mutex,
        },
        time::Duration,
    },
    tokio::{
        select,
        signal::unix::{
            signal,
            SignalKind,
        },
        spawn,
        task::JoinHandle,
        time::sleep,
    },
    tokio_util::sync::CancellationToken,
    waitgroup::WaitGroup,
};

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

    pub fn task_(
        &self,
        critical: bool,
        id: impl Into<String>,
        future: impl Future<Output = Result<(), loga::Error>> + Send + 'static,
    ) {
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
        let j = spawn(async move {
            let _w = w;
            let res = future.await;
            task_ids.lock().unwrap().remove(&id);
            if critical {
                return res.context(&format!("Critical task failed: {}", id));
            } else {
                return res;
            }
        });
        if critical {
            self.0.critical.lock().unwrap().as_mut().unwrap().push(j);
        }
    }

    /// Runs a future in the background, and terminates the manager when it exits.
    /// Callers should clone the task manager and use`if_alive` for any long `.await`s
    /// within the future so that the task is aborted if shutdown is initiated.
    pub fn critical_task(
        &self,
        id: impl Into<String>,
        future: impl Future<Output = Result<(), loga::Error>> + Send + 'static,
    ) {
        self.task_(true, id, future);
    }

    /// Runs a future in the background. Callers should clone the task manager and
    /// use`if_alive` for any long `.await`s within the future so that the task is
    /// aborted if shutdown is initiated.
    pub fn task(&self, id: impl Into<String>, future: impl Future<Output = ()> + Send + 'static) {
        self.task_(false, id, async move {
            future.await;
            return Ok(());
        })
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

    fn stream_<
        T,
        S: StreamExt<Item = T> + Send + 'static + Unpin,
        Hn: FnMut(T) -> F + Send + 'static,
        F: Future<Output = Result<(), loga::Error>> + Send + 'static,
    >(&self, critical: bool, id: impl Into<String>, mut stream: S, mut handler: Hn) {
        let id = self.prefix_id(id);
        let task_ids = self.0.alive_task_ids.clone();
        let tm = self.clone();
        let join_handle = spawn(async move {
            return loop {
                // Convoluted to work around poor rust lifetime analysis
                // https://github.com/rust-lang/rust/issues/63768
                let f = {
                    let e = select!{
                        _ = tm.until_terminate() => break Ok(()),
                        e = stream.next() => e,
                    };
                    match e {
                        Some(x) => handler(x),
                        None => break Ok(()),
                    }
                };
                let _w = match tm.0.wg.lock().unwrap().as_ref() {
                    Some(w) => w.worker(),
                    None => break Ok(()),
                };
                if !task_ids.lock().unwrap().insert(id.clone()) {
                    panic!("Task with id {} already running!", id);
                }
                let res = f.await;
                drop(_w);
                task_ids.lock().unwrap().remove(&id);
                if let Err(e) = res {
                    if critical {
                        break Err(e).context(format!("Critical task failed: {}", id));
                    } else {
                        break Err(e);
                    }
                }
            };
        });
        if critical {
            self.0.critical.lock().unwrap().as_mut().unwrap().push(join_handle);
        }
    }

    /// Calls handler for each element in stream. If the stream ends or the handler
    /// returns an error, initiates an error exit.
    pub fn critical_stream<
        T,
        S: StreamExt<Item = T> + Send + 'static + Unpin,
        Hn: FnMut(T) -> F + Send + 'static,
        F: Future<Output = Result<(), loga::Error>> + Send + 'static,
    >(&self, id: impl Into<String>, stream: S, handler: Hn) {
        self.stream_(true, id, stream, handler);
    }

    /// Calls handler for each element in stream, until the stream ends or the graceful
    /// shutdown is initiated.
    pub fn stream<
        T,
        S: StreamExt<Item = T> + Send + 'static + Unpin,
        Hn: FnMut(T) -> F + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    >(&self, id: impl Into<String>, stream: S, mut handler: Hn) {
        self.stream_(false, id, stream, move |e| {
            let t = handler(e);
            async move {
                t.await;
                return Ok(());
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
    pub async fn join(self, log: &loga::Log) -> Result<(), loga::Error> {
        let alive_ids = self.0.alive_task_ids.clone();
        let mut critical_tasks = self.0.critical.lock().unwrap().take().unwrap();
        let mut results = vec![];

        // Park until first critical task ends
        if !critical_tasks.is_empty() {
            let first_critical_task_res;
            (first_critical_task_res, _, critical_tasks) = select_all(critical_tasks).await;
            results.push(first_critical_task_res);
        }

        // Stop the rest
        let wg = self.0.wg.lock().unwrap().take().unwrap();
        self.terminate();

        // Wait for everything to finish
        let mut work = pin!(async move {
            let results = join_all(critical_tasks).await;
            wg.wait().await;
            return results;
        });
        let results1 = loop {
            select!{
                results =& mut work => break results,
                _ = sleep(std::time::Duration::from_secs(10)) => {
                    log.log_with(
                        loga::INFO,
                        "Waiting for all tasks to finish",
                        ea!(alive = (*alive_ids.lock().unwrap()).dbg_str()),
                    );
                }
            }
        };
        results.extend(results1);
        let errs = results.into_iter().filter_map(|r| match r {
            Ok(r) => match r {
                Ok(_) => None,
                Err(e) => Some(e),
            },
            Err(e) => Some(e.into()),
        }).collect::<Vec<loga::Error>>();
        if !errs.is_empty() {
            return Err(loga::agg_err("The task manager exited after critical tasks failed", errs));
        }
        return Ok(())
    }
}
