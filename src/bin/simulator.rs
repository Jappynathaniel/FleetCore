use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, ensure};
use clap::{Parser, Subcommand, ValueEnum};
use fleetcore::{
    client::{Delivery, MqttSender, retry_delay, send_http},
    protocol::{Batch, Event, MAX_BODY, MAX_EVENTS},
    transport::mqtt_options,
};
use serde_json::json;
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use uuid::Uuid;

#[derive(Parser)]
struct Config {
    #[arg(long, env = "OUTBOX_URL", default_value = "sqlite://fleetcore.sqlite")]
    outbox: String,
    #[arg(long, env = "DEVICE_ID", default_value = "north-van")]
    device: String,
    #[arg(
        long,
        env = "DEVICE_TOKEN",
        default_value = "north-van-demo",
        hide_env_values = true
    )]
    token: String,
    #[arg(long, env = "SERVICE_URL", default_value = "http://127.0.0.1:8080")]
    http: String,
    #[arg(long, env = "MQTT_HOST", default_value = "127.0.0.1")]
    mqtt: String,
    #[arg(long, env = "MQTT_PORT", default_value_t = 1883)]
    port: u16,
    #[command(subcommand)]
    action: Action,
}

#[derive(Clone, Copy, ValueEnum)]
enum Via {
    Http,
    Mqtt,
}

#[derive(Subcommand)]
enum Action {
    Generate {
        #[arg(long, default_value_t = 3)]
        events: usize,
        #[arg(long, default_value_t = 1000)]
        batch_size: usize,
        #[arg(long)]
        start_ms: Option<i64>,
    },
    Flush {
        #[arg(long, value_enum, default_value = "http")]
        via: Via,
        #[arg(long, default_value_t = 6)]
        attempts: usize,
    },
    Drive {
        #[arg(long, value_enum, default_value = "http")]
        via: Via,
    },
    Status,
    Inspect {
        #[arg(
            long,
            env = "OPERATOR_TOKEN",
            default_value = "north-operator-demo",
            hide_env_values = true
        )]
        operator: String,
    },
}

enum Transport {
    Http(reqwest::Client),
    Mqtt(Box<MqttSender>),
}

impl Transport {
    fn new(config: &Config, via: Via) -> anyhow::Result<Self> {
        Ok(match via {
            Via::Http => Self::Http(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(32))
                    .build()?,
            ),
            Via::Mqtt => {
                let options = mqtt_options(
                    format!("sim-{}-{}", config.device, Uuid::new_v4().simple()),
                    config.mqtt.clone(),
                    config.port,
                    config.device.clone(),
                    config.token.clone(),
                );
                Self::Mqtt(Box::new(MqttSender::new(options, &config.device)))
            }
        })
    }

    async fn send(&mut self, config: &Config, batch: &Batch) -> anyhow::Result<Delivery> {
        match self {
            Self::Http(client) => send_http(client, &config.http, &config.token, batch).await,
            Self::Mqtt(client) => client.send(batch).await,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let config = Config::parse();
    match &config.action {
        Action::Inspect { operator } => inspect(&config, operator).await,
        action => {
            let options = SqliteConnectOptions::from_str(&config.outbox)?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Full)
                .busy_timeout(Duration::from_secs(5));
            let db = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS outbox (
                    sequence INTEGER PRIMARY KEY, device TEXT NOT NULL, batch_id TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL, failure TEXT
                )"
            ).execute(&db).await?;
            match action {
                Action::Generate {
                    events,
                    batch_size,
                    start_ms,
                } => {
                    generate(
                        &db,
                        &config.device,
                        *events,
                        *batch_size,
                        start_ms.unwrap_or_else(now_ms),
                    )
                    .await?;
                    status(&db, &config.device).await
                }
                Action::Flush { via, attempts } => {
                    ensure!(
                        (1..=20).contains(attempts),
                        "attempts must be between 1 and 20"
                    );
                    flush(&db, &config, &mut Transport::new(&config, *via)?, *attempts).await?;
                    status(&db, &config.device).await
                }
                Action::Status => status(&db, &config.device).await,
                Action::Drive { via } => drive(&db, &config, *via).await,
                Action::Inspect { .. } => unreachable!(),
            }
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn generate(
    db: &SqlitePool,
    device: &str,
    count: usize,
    batch_size: usize,
    start_ms: i64,
) -> anyhow::Result<()> {
    ensure!(
        count > 0 && (1..=MAX_EVENTS).contains(&batch_size),
        "invalid event or batch count"
    );
    let end = (count - 1)
        .checked_mul(10_000)
        .and_then(|offset| i64::try_from(offset).ok())
        .and_then(|offset| start_ms.checked_add(offset));
    ensure!(
        start_ms >= 0 && end.is_some_and(|last| last <= 253_402_300_799_999),
        "timestamps out of range"
    );
    for offset in (0..count).step_by(batch_size) {
        let batch = Batch {
            batch_id: Uuid::new_v4(),
            events: (offset..offset + batch_size.min(count - offset))
                .map(|index| Event {
                    event_id: Uuid::new_v4(),
                    observed_at_ms: start_ms + index as i64 * 10_000,
                    latitude_e6: 12_971_599 + (index % 100) as i32,
                    longitude_e6: 77_594_566,
                    speed_cm_s: (index % 2500) as i32,
                })
                .collect(),
        };
        let payload = serde_json::to_string(&batch)?;
        ensure!(
            payload.len() <= MAX_BODY,
            "encoded batch exceeds the service limit"
        );
        sqlx::query("INSERT INTO outbox (device, batch_id, payload) VALUES (?, ?, ?)")
            .bind(device)
            .bind(batch.batch_id.to_string())
            .bind(payload)
            .execute(db)
            .await?;
    }
    Ok(())
}

async fn flush(
    db: &SqlitePool,
    config: &Config,
    transport: &mut Transport,
    attempts: usize,
) -> anyhow::Result<()> {
    while let Some((id, payload)) = sqlx::query_as::<_, (String, String)>(
        "SELECT batch_id, payload FROM outbox WHERE device = ? AND failure IS NULL ORDER BY sequence LIMIT 1"
    ).bind(&config.device).fetch_optional(db).await? {
        let batch: Batch = serde_json::from_str(&payload).context("decode persisted batch")?;
        let mut completed = false;
        for attempt in 0..attempts {
            match transport.send(config, &batch).await {
                Ok(Delivery::Acknowledged(receipt)) => {
                    sqlx::query("DELETE FROM outbox WHERE batch_id = ?")
                        .bind(&id).execute(db).await?;
                    println!("{}", serde_json::to_string(&receipt)?);
                    completed = true;
                    break;
                }
                Ok(Delivery::Rejected(reason)) => {
                    sqlx::query("UPDATE outbox SET failure = ? WHERE batch_id = ?")
                        .bind(&reason).bind(&id).execute(db).await?;
                    tracing::error!(batch_id = %id, %reason, "batch was rejected");
                    completed = true;
                    break;
                }
                Ok(Delivery::Retry) => {}
                Err(error) => tracing::warn!(%error, "delivery attempt failed"),
            }
            if attempt + 1 < attempts {
                tokio::time::sleep(retry_delay(attempt)).await;
            }
        }
        ensure!(completed, "batch {id} remains queued after {attempts} attempts");
    }
    let rejected: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox WHERE device = ? AND failure IS NOT NULL")
            .bind(&config.device)
            .fetch_one(db)
            .await?;
    ensure!(
        rejected == 0,
        "{rejected} permanently rejected batches remain in the outbox"
    );
    Ok(())
}

async fn drive(db: &SqlitePool, config: &Config, via: Via) -> anyhow::Result<()> {
    let mut transport = Transport::new(config, via)?;
    let record = async {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            generate(db, &config.device, 1, 1, now_ms()).await?;
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    };
    let replay = async {
        loop {
            if let Err(error) = flush(db, config, &mut transport, 6).await {
                tracing::warn!(%error, "outbox needs attention");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    tokio::select! {
        result = record => result,
        _ = replay => Ok(()),
        result = tokio::signal::ctrl_c() => Ok(result?),
    }
}

async fn status(db: &SqlitePool, device: &str) -> anyhow::Result<()> {
    let (pending, rejected): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE failure IS NULL), count(*) FILTER (WHERE failure IS NOT NULL)
         FROM outbox WHERE device = ?"
    ).bind(device).fetch_one(db).await?;
    println!(
        "{}",
        json!({"device": device, "pending_batches": pending, "rejected_batches": rejected})
    );
    Ok(())
}

async fn inspect(config: &Config, operator: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    for route in ["latest", "telemetry?limit=10"] {
        let response = client
            .get(format!(
                "{}/v1/vehicles/{}/{route}",
                config.http, config.device
            ))
            .bearer_auth(operator)
            .send()
            .await?
            .error_for_status()?;
        println!("{route} {}", response.text().await?);
    }
    Ok(())
}
