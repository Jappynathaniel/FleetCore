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
    #[arg(long, default_value_t = 96)]
    concurrency: usize,
    #[arg(long, default_value_t = 0)]
    sustain_seconds: u64,
    #[arg(long, default_value_t = 0)]
    query_rate: u64,
    #[arg(long)]
    devices: Option<usize>,
    #[arg(long)]
    events_per_device: Option<usize>,
    #[arg(long, default_value_t = 0)]
    arrival_spread_ms: u64,
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
    scheduled_elapsed: Duration,
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
    scheduled_p50_ms: f64,
    scheduled_p95_ms: f64,
    scheduled_p99_ms: f64,
    integrity_verified: bool,
    concurrency: usize,
    mean_stage_ms: BTreeMap<String, f64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    ensure!(
        (1..=1024).contains(&config.concurrency),
        "concurrency must be 1–1024"
    );
    if config.sustain_seconds > 0 {
        ensure!(
            matches!(config.scenario, Some(Scenario::Live))
                && matches!(config.layer, Some(Layer::Http))
                && config.mode.is_some(),
            "sustained mode requires --scenario live --layer http and one --mode"
        );
    }
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
        "measured_trials": config.trials, "database_concurrency": 8, "http_concurrency": config.concurrency, "latency_origin": "scheduled arrival plus first attempt",
        "live_rate_per_second": 100, "event_seed": "fleetcore-v1",
        "devices_override": config.devices, "events_per_device_override": config.events_per_device,
        "arrival_spread_ms": config.arrival_spread_ms, "query_rate": config.query_rate,
        "sustain_seconds": config.sustain_seconds})
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

    let study_started = Instant::now();
    let mut epoch = 0_u64;
    loop {
        for &scenario in &scenarios {
            let mut inputs = prepare(&db, scenario, &config).await?;
            for input in &mut inputs {
                for event in &mut input.batch.events {
                    if config.sustain_seconds > 0 {
                        event.event_id = Uuid::new_v4();
                        event.observed_at_ms += epoch as i64 * 100_000_000;
                    }
                }
            }
            for input in &mut inputs {
                input.batch.events.sort_by_key(|event| event.event_id);
            }
            for &layer in &layers {
                for trial in 0..=if config.sustain_seconds > 0 {
                    0
                } else {
                    config.trials
                } {
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
                            &config, epoch,
                        )
                        .await?;
                        println!("{}", serde_json::to_string(&result)?);
                        if config.sustain_seconds > 0 {
                            eprintln!(
                                "{}",
                                json!({"epoch": epoch, "study_wall_seconds": study_started.elapsed().as_secs_f64()})
                            );
                        }
                    }
                }
            }
        }
        epoch += 1;
        if config.sustain_seconds == 0
            || study_started.elapsed().as_secs() >= config.sustain_seconds
        {
            break;
        }
    }
    sqlx::query("SELECT pg_advisory_unlock(719280771)")
        .execute(&mut *guard)
        .await?;
    Ok(())
}

async fn prepare(db: &PgPool, scenario: Scenario, config: &Config) -> anyhow::Result<Vec<Input>> {
    let (default_devices, default_batch) = scenario.dimensions();
    let devices = config.devices.unwrap_or(default_devices);
    let per_batch = config.events_per_device.unwrap_or(default_batch);
    ensure!(
        (1..=10000).contains(&devices) && (1..=10000).contains(&per_batch),
        "invalid workload dimensions"
    );
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
            batch_id: Uuid::new_v4(),
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
            Duration::from_millis(if config.arrival_spread_ms == 0 {
                0
            } else {
                (device_index as u64 * 2654435761) % config.arrival_spread_ms
            })
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
    inputs.sort_by_key(|input| input.offset);
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
    config: &Config,
    epoch: u64,
) -> anyhow::Result<ResultRow<'a>> {
    if config.sustain_seconds == 0 || epoch == 0 && trial == 0 {
        sqlx::query("TRUNCATE telemetry").execute(db).await?;
    }
    let expected: usize = inputs.iter().map(|input| input.batch.events.len()).sum();
    let prior_count: i64 = sqlx::query_scalar("SELECT count(*) FROM telemetry")
        .fetch_one(db)
        .await?;
    let before_metrics = match layer {
        Layer::Http => scrape(http, url).await?,
        Layer::Database => Stage::new(),
    };
    let before: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(db)
        .await?;
    let query_stop = tokio_util::sync::CancellationToken::new();
    let queries = if config.query_rate > 0 {
        let client = http.clone();
        let base = url.to_owned();
        let rate = config.query_rate;
        let stop = query_stop.clone();
        Some(tokio::spawn(async move {
            let began_run = Instant::now();
            let mut latencies = Vec::new();
            let mut errors = 0;
            let mut interval = tokio::time::interval(Duration::from_secs_f64(1.0 / rate as f64));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = interval.tick() => {}
                }
                let began = Instant::now();
                let route = if latencies.len() % 2 == 0 {
                    "latest"
                } else {
                    "telemetry?limit=100"
                };
                let result = async {
                    let response = client
                        .get(format!("{base}/v1/vehicles/bench-0000/{route}"))
                        .bearer_auth("benchmark-only")
                        .send()
                        .await?
                        .error_for_status()?;
                    // Empty latest is a valid HTTP 204 during the initial ingestion.
                    if response.status() != reqwest::StatusCode::NO_CONTENT {
                        let value = response.json::<serde_json::Value>().await?;
                        if !(value.get("event_id").is_some()
                            || value.get("events").is_some_and(|v| v.is_array()))
                        {
                            anyhow::bail!("invalid query response");
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await;
                if result.is_err() {
                    errors += 1;
                }
                latencies.push(began.elapsed().as_secs_f64() * 1000.0);
            }
            latencies.sort_unstable_by(f64::total_cmp);
            eprintln!(
                "{}",
                json!({"query_samples": latencies.len(), "query_errors": errors,
                "query_elapsed_seconds": began_run.elapsed().as_secs_f64(),
                "observed_query_rate": latencies.len() as f64 / began_run.elapsed().as_secs_f64(),
                "query_p99_ms": if latencies.is_empty() { 0.0 } else { percentile(&latencies, 0.99) }})
            );
        }))
    } else {
        None
    };
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
                        scheduled_elapsed: write.database_time + write.commit_time,
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
                                    scheduled_elapsed: (start + input.offset).elapsed(),
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
                .buffer_unordered(config.concurrency)
                .try_collect()
                .await?
        }
    };
    let elapsed = start.elapsed().as_secs_f64();
    query_stop.cancel();
    if let Some(task) = queries {
        task.await?;
    }
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
        count == prior_count + expected as i64,
        "database contains {count} rows, expected {expected}"
    );
    // Verify every field outside the timed interval with bounded per-device reads.
    for input in inputs {
        let minimum = input
            .batch
            .events
            .iter()
            .map(|event| event.observed_at_ms)
            .min()
            .unwrap();
        let maximum = input
            .batch
            .events
            .iter()
            .map(|event| event.observed_at_ms)
            .max()
            .unwrap();
        let mut actual: Vec<Event> = sqlx::query_as(
            "SELECT event_id, observed_at_ms, latitude_e6, longitude_e6, speed_cm_s
             FROM telemetry WHERE tenant_id = $1 AND device_id = $2
             AND observed_at_ms BETWEEN $3 AND $4 ORDER BY event_id",
        )
        .bind(&input.device.tenant_id)
        .bind(&input.device.id)
        .bind(minimum)
        .bind(maximum)
        .fetch_all(db)
        .await?;
        actual.sort_by_key(|event| event.event_id);
        ensure!(
            actual == input.batch.events,
            "event integrity mismatch for {}",
            input.device.id
        );
    }
    let mut scheduled: Vec<_> = samples
        .iter()
        .map(|sample| sample.scheduled_elapsed.as_secs_f64() * 1000.0)
        .collect();
    scheduled.sort_unstable_by(f64::total_cmp);
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
        warmup: trial == 0 && epoch == 0,
        events: expected,
        batches: samples.len(),
        elapsed_seconds: elapsed,
        events_per_second: expected as f64 / elapsed,
        wal_bytes,
        confirmation_p50_ms: percentile(&latencies, 0.50),
        confirmation_p95_ms: percentile(&latencies, 0.95),
        confirmation_p99_ms: percentile(&latencies, 0.99),
        retries: samples.iter().map(|s| s.retries).sum(),
        scheduled_p50_ms: percentile(&scheduled, 0.50),
        scheduled_p95_ms: percentile(&scheduled, 0.95),
        scheduled_p99_ms: percentile(&scheduled, 0.99),
        integrity_verified: true,
        concurrency: config.concurrency,
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
