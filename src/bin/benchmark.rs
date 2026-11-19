use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};
use clap::{Parser, ValueEnum};
use fleetcore::{
    client::{Delivery, retry_delay, send_http},
    protocol::{Batch, Event},
    store::{Device, Store, WriteMode},
};
use futures_util::{StreamExt, TryStreamExt, stream};
use reqwest::Client;
use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum Scenario {
    Live,
    Reconnect,
    Large,
}

impl Scenario {
    fn dimensions(self) -> (usize, usize) {
        match self {
            Self::Live => (1000, 1),
            Self::Reconnect => (1000, 360),
            Self::Large => (100, 10_000),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum Layer {
    Database,
    Http,
}

#[derive(Parser)]
struct Config {
    #[arg(long, env = "BENCH_DATABASE_URL", hide_env_values = true)]
    database: String,
    #[arg(long, env = "BENCH_UNNEST_URL")]
    unnest_url: String,
    #[arg(long, env = "BENCH_COPY_URL")]
    copy_url: String,
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u8).range(1..=20))]
    trials: u8,
    #[arg(long, value_enum)]
    scenario: Option<Scenario>,
    #[arg(long, value_enum)]
    layer: Option<Layer>,
    #[arg(long, value_enum)]
    mode: Option<WriteMode>,
}

struct Input {
    device: Device,
    token: String,
    batch: Batch,
    offset: Duration,
}

#[derive(Default)]
struct Sample {
    elapsed: Duration,
    database: Duration,
    commit: Duration,
    retries: usize,
}

type Stage = BTreeMap<String, StageValue>;

#[derive(Clone, Copy, Default)]
struct StageValue {
    sum_seconds: f64,
    count: u64,
}

#[derive(Serialize)]
struct ResultRow<'a> {
    layer: Layer,
    scenario: Scenario,
    mode: &'a str,
    trial: u8,
    warmup: bool,
    events: usize,
    batches: usize,
    elapsed_seconds: f64,
    events_per_second: f64,
    wal_bytes: i64,
    confirmation_p50_ms: f64,
    confirmation_p95_ms: f64,
    confirmation_p99_ms: f64,
    retries: usize,
    mean_stage_ms: BTreeMap<String, f64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    let db = PgPool::connect(&config.database).await?;
    let mut guard = db.acquire().await?;
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut *guard)
        .await?;
    ensure!(
        name == "fleetcore_benchmark",
        "benchmark requires the dedicated fleetcore_benchmark database"
    );
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(719280771)")
        .fetch_one(&mut *guard)
        .await?;
    ensure!(acquired, "another benchmark is using this database");

    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&db)
        .await?;
    let durability: String = sqlx::query_scalar("SHOW synchronous_commit")
        .fetch_one(&db)
        .await?;
    ensure!(
        durability == "on",
        "benchmark requires synchronous_commit = on"
    );
    println!(
        "{}",
        json!({"postgresql": version, "synchronous_commit": durability,
        "measured_trials": config.trials, "database_concurrency": 8, "http_concurrency": 96,
        "live_rate_per_second": 100, "event_seed": "fleetcore-v1"})
    );

    let unnest = Store::open(&config.database, WriteMode::Unnest, 12).await?;
    let copy = Store::open(&config.database, WriteMode::Copy, 12).await?;
    let http = Client::builder().timeout(Duration::from_secs(32)).build()?;
    let scenarios = config.scenario.map_or_else(
        || vec![Scenario::Live, Scenario::Reconnect, Scenario::Large],
        |v| vec![v],
    );
    let layers = config
        .layer
        .map_or_else(|| vec![Layer::Database, Layer::Http], |v| vec![v]);

    for scenario in scenarios {
        let inputs = prepare(&db, scenario).await?;
        for &layer in &layers {
            for trial in 0..=config.trials {
                let order = if trial % 2 == 0 {
                    [WriteMode::Unnest, WriteMode::Copy]
                } else {
                    [WriteMode::Copy, WriteMode::Unnest]
                };
                for mode in order {
                    if config.mode.is_some_and(|selected| selected != mode) {
                        continue;
                    }
                    let (store, url, mode_name) = match mode {
                        WriteMode::Unnest => (&unnest, &config.unnest_url, "unnest"),
                        WriteMode::Copy => (&copy, &config.copy_url, "copy"),
                    };
                    let result = run(
                        &db, layer, scenario, &inputs, store, &http, url, mode_name, trial,
                    )
                    .await?;
                    println!("{}", serde_json::to_string(&result)?);
                }
            }
        }
    }
    sqlx::query("SELECT pg_advisory_unlock(719280771)")
        .execute(&mut *guard)
        .await?;
    Ok(())
}

async fn prepare(db: &PgPool, scenario: Scenario) -> anyhow::Result<Vec<Input>> {
    let (devices, per_batch) = scenario.dimensions();
    let mut tx = db.begin().await?;
    sqlx::query("INSERT INTO tenants VALUES ('benchmark', sha256(convert_to('benchmark-only', 'UTF8'))) ON CONFLICT DO NOTHING")
        .execute(&mut *tx).await?;
    let mut inputs = Vec::with_capacity(devices);
    for device_index in 0..devices {
        let id = format!("bench-{device_index:04}");
        let token = format!("{id}-sample");
        sqlx::query("INSERT INTO devices VALUES ($1, 'benchmark', sha256(convert_to($2, 'UTF8'))) ON CONFLICT DO NOTHING")
            .bind(&id).bind(&token).execute(&mut *tx).await?;
        let mut batch = Batch {
            batch_id: deterministic_id(1_000_001 + device_index as u128),
            events: (0..per_batch)
                .map(|row| Event {
                    event_id: deterministic_id((device_index * per_batch + row + 1) as u128),
                    observed_at_ms: 1_740_000_000_000 + row as i64 * 10_000,
                    latitude_e6: 12_971_599 + (row % 100) as i32,
                    longitude_e6: 77_594_566 + (device_index % 100) as i32,
                    speed_cm_s: (row % 2500) as i32,
                })
                .collect(),
        };
        batch
            .validate()
            .map_err(|outcome| anyhow::anyhow!("invalid synthetic input: {outcome:?}"))?;
        let offset = if matches!(scenario, Scenario::Live) {
            Duration::from_millis(device_index as u64 * 10)
        } else {
            Duration::ZERO
        };
        inputs.push(Input {
            device: Device {
                id,
                tenant_id: "benchmark".into(),
            },
            token,
            batch,
            offset,
        });
    }
    tx.commit().await?;
    Ok(inputs)
}

fn deterministic_id(seq: u128) -> Uuid {
    Uuid::from_u128(seq.wrapping_mul(0x137e132bd168caa784ea1c9802ac6b41))
}

#[allow(clippy::too_many_arguments)]
async fn run<'a>(
    db: &PgPool,
    layer: Layer,
    scenario: Scenario,
    inputs: &[Input],
    store: &Store,
    http: &Client,
    url: &str,
    mode: &'a str,
    trial: u8,
) -> anyhow::Result<ResultRow<'a>> {
    sqlx::query("TRUNCATE telemetry").execute(db).await?;
    let expected: usize = inputs.iter().map(|input| input.batch.events.len()).sum();
    let before_metrics = match layer {
        Layer::Http => scrape(http, url).await?,
        Layer::Database => Stage::new(),
    };
    let before: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(db)
        .await?;
    let start = Instant::now();
    let samples: Vec<Sample> = match layer {
        Layer::Database => {
            stream::iter(inputs)
                .map(|input| async move {
                    let write = store
                        .write(&input.device, &input.batch.events)
                        .await
                        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
                    ensure!(
                        write.inserted == input.batch.events.len(),
                        "database write did not insert the expected rows"
                    );
                    Ok::<_, anyhow::Error>(Sample {
                        elapsed: write.database_time + write.commit_time,
                        database: write.database_time,
                        commit: write.commit_time,
                        retries: 0,
                    })
                })
                .buffer_unordered(8)
                .try_collect()
                .await?
        }
        Layer::Http => {
            stream::iter(inputs)
                .map(|input| async move {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(start + input.offset))
                        .await;
                    let began = Instant::now();
                    let mut error = String::new();
                    for attempt in 0..12 {
                        match send_http(http, url, &input.token, &input.batch).await {
                            Ok(Delivery::Acknowledged(receipt)) => {
                                ensure!(
                                    receipt.inserted + receipt.duplicates
                                        == input.batch.events.len(),
                                    "incomplete receipt"
                                );
                                return Ok(Sample {
                                    elapsed: began.elapsed(),
                                    retries: attempt,
                                    ..Sample::default()
                                });
                            }
                            Ok(Delivery::Retry) => {}
                            Ok(Delivery::Rejected(reason)) => {
                                bail!("benchmark batch rejected: {reason}")
                            }
                            Err(err) => error = err.to_string(),
                        }
                        if attempt < 11 {
                            tokio::time::sleep(retry_delay(attempt)).await;
                        }
                    }
                    bail!("batch {} exhausted retries: {error}", input.batch.batch_id)
                })
                .buffer_unordered(96)
                .try_collect()
                .await?
        }
    };
    let elapsed = start.elapsed().as_secs_f64();
    let wal_bytes: i64 = sqlx::query_scalar(
        "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::text::pg_lsn)::bigint",
    )
    .bind(before)
    .fetch_one(db)
    .await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM telemetry")
        .fetch_one(db)
        .await?;
    ensure!(
        count == expected as i64,
        "database contains {count} rows, expected {expected}"
    );
    let mut latencies: Vec<_> = samples
        .iter()
        .map(|sample| sample.elapsed.as_secs_f64() * 1000.0)
        .collect();
    latencies.sort_unstable_by(f64::total_cmp);
    let mean_stage_ms = match layer {
        Layer::Http => {
            let after = scrape(http, url).await?;
            after
                .into_iter()
                .filter_map(|(key, end)| {
                    let before = before_metrics.get(&key).copied().unwrap_or_default();
                    let n = end.count.checked_sub(before.count)?;
                    (n > 0).then(|| {
                        (
                            key,
                            (end.sum_seconds - before.sum_seconds) * 1000.0 / n as f64,
                        )
                    })
                })
                .collect()
        }
        Layer::Database => {
            let mean = |time: fn(&Sample) -> Duration| {
                samples
                    .iter()
                    .map(|sample| time(sample).as_secs_f64() * 1000.0)
                    .sum::<f64>()
                    / samples.len() as f64
            };
            BTreeMap::from([
                ("database".into(), mean(|s| s.database)),
                ("commit".into(), mean(|s| s.commit)),
            ])
        }
    };
    Ok(ResultRow {
        layer,
        scenario,
        mode,
        trial,
        warmup: trial == 0,
        events: expected,
        batches: samples.len(),
        elapsed_seconds: elapsed,
        events_per_second: expected as f64 / elapsed,
        wal_bytes,
        confirmation_p50_ms: percentile(&latencies, 0.50),
        confirmation_p95_ms: percentile(&latencies, 0.95),
        confirmation_p99_ms: percentile(&latencies, 0.99),
        retries: samples.iter().map(|s| s.retries).sum(),
        mean_stage_ms,
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[(p * sorted.len() as f64).ceil() as usize - 1]
}

async fn scrape(http: &Client, base: &str) -> anyhow::Result<Stage> {
    let text = http
        .get(format!("{base}/metrics"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let mut stages = Stage::new();
    for line in text.lines().filter(|line| {
        line.starts_with("fleetcore_stage_seconds_") && line.contains(r#"transport="http""#)
    }) {
        let Some((_, rest)) = line.split_once(r#"stage=""#) else {
            continue;
        };
        let (stage, _) = rest.split_once('"').context("unterminated stage label")?;
        let (_, value) = line.rsplit_once(' ').context("missing stage value")?;
        if line.starts_with("fleetcore_stage_seconds_sum{") {
            stages.entry(stage.into()).or_default().sum_seconds = value.parse()?;
        } else if line.starts_with("fleetcore_stage_seconds_count{") {
            stages.entry(stage.into()).or_default().count = value.parse()?;
        }
    }
    Ok(stages)
}
