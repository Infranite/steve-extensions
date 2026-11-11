use std::io::Read;
use std::process::ExitCode;

use clap::Parser;
use steve_plugin_openlark::config::{Args, Settings};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;

/// Resolves when standard input reaches its end: the Hub that started the plugin is gone.
///
/// Read on a plain thread, which does not keep the process from exiting the way a pending
/// asynchronous read of standard input would.
fn stdin_closed() -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut buf = [0u8; 1024];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) => break,
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
        let _ = tx.send(());
    });
    rx
}

/// Resolves when the plugin is asked to stop.
async fn shutdown(supervised: bool) {
    let hub_gone = async {
        if supervised {
            let _ = stdin_closed().await;
        } else {
            // started by hand or by a service manager: standard input says nothing
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminated = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut term) => {
                term.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminated = std::future::pending::<()>();
    let reason = tokio::select! {
        () = hub_gone => "standard input closed",
        () = terminated => "SIGTERM",
        _ = tokio::signal::ctrl_c() => "SIGINT",
    };
    tracing::info!("stopping: {reason}");
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_writer(std::io::stderr).with_env_filter(filter).init();

    let (addr, settings) = match Settings::load(&args) {
        Ok(loaded) => loaded,
        Err(e) => {
            tracing::error!("cannot start: {e}");
            return ExitCode::from(2);
        }
    };
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("cannot listen on {addr}: {e}");
            return ExitCode::from(2);
        }
    };
    // with port 0 this line is how the caller learns the port
    match listener.local_addr() {
        Ok(local) => tracing::info!("serving the Hub on ws://{local}/ws"),
        Err(_) => tracing::info!("serving the Hub on ws://{addr}/ws"),
    }
    match steve_plugin_openlark::run(listener, settings, shutdown(args.supervised)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("the server failed: {e}");
            ExitCode::FAILURE
        }
    }
}
