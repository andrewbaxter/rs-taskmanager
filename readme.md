This is a tool for managing asynchronous tasks.

For example, maybe you have some periodic background tasks, plus a couple web servers. Using a `TaskManager` you can group these and shut them down as a group, gracefully, and wait for them all to end.

Handles

- Long tasks
- Simple periodic tasks (fixed sleep between invocations)
- Async streams
- Ctrl+c shutdown

Use it like

```rust
let tm = TaskManager::new();
tm.attach_sigint({
    log2 = logger.clone();
    |e| log2.err("Error handling sigint", e)
});
let tm1 = tm.clone();
let log2 = logger.clone();
tm.task(async move {
    match tm1.if_alive(server).await {
        Some(r) => match r {
            Ok(_) => {} // Server exited normally
            Err(e) => {
                log.err("Server died with error", e);
            }
        },
        None => {} // Received shutdown
    };
});

tm.join().await;
```
