use std::{
    net::SocketAddr,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use anyhow::{Context, ensure};
use clap::Parser;
use fleetcore::{
    admission::Admission,
    metrics::Metrics,
    service::{App, Ingestion},
    store::{Store, WriteMode},
    transport::{mqtt_options, router, run_mqtt},
};
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
struct Config {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,
    #[arg(long, env = "HTTP_BIND", default_value = "0.0.0.0:8080")]
    listen: SocketAddr,
    #[arg(long, env = "INGEST_MODE", value_enum, default_value = "unnest")]
    mode: WriteMode,
    #[arg(long, env = "INGEST_WORKERS", default_value_t = 8)]
    workers: usize,
    #[arg(long, env = "INGEST_QUEUE", default_value_t = 64)]
    queue: usize,
    #[arg(long, env = "DATABASE_CONNECTIONS", default_value_t = 12)]
    connections: u32,
    #[arg(long, env = "MQTT_HOST")]
    mqtt_host: Option<String>,
    #[arg(long, env = "MQTT_PORT", default_value_t = 1883)]
    mqtt_port: u16,
    #[arg(long, env = "MQTT_USERNAME", default_value = "fleetcore-service")]
    mqtt_username: String,
    #[arg(long, env = "MQTT_PASSWORD", hide_env_values = true)]
    mqtt_password: Option<String>,
    #[arg(long, env = "SEED_DEMO", default_value_t = false)]
    seed_demo: bool,
    #[arg(long, env = "TENANT_ADMISSION", default_value_t = false)]
    tenant_admission: bool,
    #[arg(long, env = "ADMISSION_GLOBAL", default_value_t = 16)]
    admission_global: usize,
    #[arg(long, env = "ADMISSION_PER_TENANT", default_value_t = 8)]
    admission_per_tenant: usize,
    #[arg(long, env = "BODY_TIMEOUT_SECONDS", default_value_t = 3)]
    body_timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = Config::parse();
    ensure!(
        config.workers > 0 && config.queue > 0,
        "workers and queue must be positive"
    );
    ensure!(
        config.connections as usize > config.workers,
        "reserve database connections for reads and identity"
    );
    ensure!(
        config.admission_global > 0
            && config.admission_per_tenant > 0
            && config.admission_per_tenant <= config.admission_global
            && config.body_timeout_seconds > 0,
        "invalid admission or body timeout configuration"
    );
    let store = Store::open(&config.database_url, config.mode, config.connections).await?;
    if config.seed_demo {
        store.seed_demo().await?;
    }

    let listener = TcpListener::bind(config.listen).await?;
    let metrics = Metrics::new()?;
    let admission = Admission::new(
        config.tenant_admission,
        config.admission_global,
        config.admission_per_tenant,
        &metrics,
    );
    let (ingestion, mut dispatcher) = Ingestion::start_with_admission(
        store.clone(),
        metrics,
        config.queue,
        config.workers,
        admission,
    );
    let mqtt_stop = CancellationToken::new();
    let mqtt_ready = Arc::new(AtomicBool::new(config.mqtt_host.is_none()));
    let mqtt_task = if let Some(host) = config.mqtt_host {
        let password = config
            .mqtt_password
            .context("MQTT_PASSWORD is required when MQTT_HOST is set")?;
        let options = mqtt_options(
            "fleetcore-service".into(),
            host,
            config.mqtt_port,
            config.mqtt_username,
            password,
        );
        let ingestion = ingestion.clone();
        let ready = mqtt_ready.clone();
        let stop = mqtt_stop.clone();
        Some(tokio::spawn(async move {
            if let Err(error) = run_mqtt(options, ingestion, ready, stop).await {
                tracing::error!(%error, "MQTT transport stopped");
            }
        }))
    } else {
        None
    };

    let app = App {
        store,
        ingestion: ingestion.clone(),
        http_admission: Arc::new(Semaphore::new(config.workers + config.queue)),
        mqtt_ready,
        body_timeout: Duration::from_secs(config.body_timeout_seconds),
    };
    let stop = ingestion.stopping.clone();
    let mut http = tokio::spawn(async move {
        axum::serve(listener, router(app))
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
    });
    tracing::info!(address = %config.listen, mode = ?config.mode, "FleetCore listening");

    let early_exit = tokio::select! {
        result = &mut http => Some(result),
        _ = shutdown_signal() => None,
    };
    ingestion.stopping.cancel();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    if tokio::time::timeout_at(deadline, &mut dispatcher)
        .await
        .is_err()
    {
        tracing::warn!("ingestion drain timed out; pending clients can replay");
        dispatcher.abort();
    }
    mqtt_stop.cancel();
    if let Some(task) = mqtt_task {
        let _ = task.await;
    }
    if let Some(result) = early_exit {
        result??;
    } else {
        match tokio::time::timeout_at(deadline, &mut http).await {
            Ok(result) => result??,
            Err(_) => http.abort(),
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
