use std::time::{Duration, Instant};

use anyhow::{Context, bail, ensure};
use clap::Parser;
use fleetcore::{
    client::{Delivery, MqttSender},
    metrics::Metrics,
    protocol::{Batch, Event as TelemetryEvent, Outcome, Receipt},
    service::{Identity, Ingestion, Reply},
    store::{Store, WriteMode},
    transport::mqtt_options,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use reqwest::{Client, Response, StatusCode};
use rumqttc::{AsyncClient, QoS};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

const NORTH_DEVICE: &str = "north-van";
const NORTH: &str = "north-van-demo";
const OPERATOR: &str = "north-operator-demo";

#[derive(Parser)]
struct Config {
    #[arg(long, env = "SERVICE_URL", default_value = "http://127.0.0.1:8080")]
    http: String,
    #[arg(long, env = "MQTT_HOST", default_value = "127.0.0.1")]
    broker: String,
    #[arg(long, env = "DATABASE_URL")]
    database: String,
    #[arg(long, env = "INGEST_MODE", value_enum, default_value = "unnest")]
    mode: WriteMode,
}

struct Harness {
    config: Config,
    http: Client,
}

#[derive(Deserialize)]
struct Page {
    events: Vec<TelemetryEvent>,
    next_cursor: Option<String>,
}

impl Harness {
    async fn post(&self, token: &str, batch: &Batch) -> anyhow::Result<Response> {
        Ok(self
            .http
            .post(format!("{}/v1/telemetry/batches", self.config.http))
            .bearer_auth(token)
            .json(batch)
            .send()
            .await?)
    }

    async fn read(&self, resource: &str, operator: &str) -> anyhow::Result<Response> {
        Ok(self
            .http
            .get(format!("{}/v1/vehicles/{resource}", self.config.http))
            .bearer_auth(operator)
            .send()
            .await?)
    }

    async fn accept(&self, batch: &Batch) -> anyhow::Result<Receipt> {
        let result = self
            .post(NORTH, batch)
            .await?
            .error_for_status()?
            .json::<Receipt>()
            .await?;
        ensure!(
            result.batch_id == batch.batch_id && result.outcome == Outcome::Committed,
            "wrong receipt"
        );
        Ok(result)
    }

    fn options(&self) -> rumqttc::MqttOptions {
        mqtt_options(
            format!("test-{}", Uuid::new_v4().simple()),
            self.config.broker.clone(),
            1883,
            NORTH_DEVICE.into(),
            NORTH.into(),
        )
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    let database = PgPool::connect(&config.database).await?;
    let harness = Harness {
        config,
        http: Client::builder().timeout(Duration::from_secs(20)).build()?,
    };
    let mut stream = MqttSender::new(harness.options(), NORTH_DEVICE);
    let run = Uuid::new_v4();
    let registry = sqlx::query("INSERT INTO tenants VALUES ($1, sha256(convert_to($2, 'UTF8')))")
        .bind(run.to_string())
        .bind(run.to_string())
        .execute(&database)
        .await?;
    ensure!(
        registry.rows_affected() == 1,
        "test database is not writable"
    );

    ensure!(
        harness
            .post("wrong", &batch(vec![event(1)]))
            .await?
            .status()
            == StatusCode::UNAUTHORIZED
    );
    ensure!(harness.read("south-van/latest", OPERATOR).await?.status() == StatusCode::NOT_FOUND);

    let newest = event(253_402_300_799_999);
    let older = event(1_000);
    let initial = batch(vec![newest.clone(), older.clone()]);
    ensure!(harness.accept(&initial).await?.inserted == 2);
    ensure!(harness.accept(&initial).await?.duplicates == 2);
    let bad = TelemetryEvent {
        latitude_e6: 91_000_000,
        ..event(5_000)
    };
    ensure!(harness.post(NORTH, &batch(vec![bad])).await?.status() == StatusCode::BAD_REQUEST);
    let extra = event(7_000);
    let mismatch = TelemetryEvent {
        speed_cm_s: older.speed_cm_s + 1,
        ..older.clone()
    };
    ensure!(
        harness
            .post(NORTH, &batch(vec![extra.clone(), mismatch]))
            .await?
            .status()
            == StatusCode::CONFLICT
    );
    ensure!(
        count(&database, extra.event_id).await? == 0,
        "conflicting batch partially committed"
    );

    let late = batch(vec![event(2_000)]);
    match stream.send(&late).await? {
        Delivery::Acknowledged(receipt) => ensure!(receipt.inserted == 1, "MQTT event absent"),
        _ => bail!("MQTT failed to produce a committed receipt"),
    }
    let latest = harness
        .read("north-van/latest", OPERATOR)
        .await?
        .error_for_status()?
        .json::<TelemetryEvent>()
        .await?;
    ensure!(
        latest.observed_at_ms == newest.observed_at_ms,
        "late arrival moved latest backwards"
    );
    let first = harness
        .read("north-van/telemetry?limit=1", OPERATOR)
        .await?
        .error_for_status()?
        .json::<Page>()
        .await?;
    let cursor = first.next_cursor.context("missing history cursor")?;
    let next = harness
        .read(
            &format!("north-van/telemetry?limit=1&cursor={cursor}"),
            OPERATOR,
        )
        .await?
        .error_for_status()?
        .json::<Page>()
        .await?;
    ensure!(
        first.events[0].event_id != next.events[0].event_id,
        "history repeated the cursor row"
    );

    let by_http = harness.accept(&late).await?;
    ensure!(
        by_http.duplicates == 1,
        "HTTP and MQTT deduplication diverged"
    );
    let concurrent = batch(vec![event(8_000)]);
    let (one, two) = tokio::try_join!(harness.accept(&concurrent), harness.accept(&concurrent))?;
    ensure!(one.inserted + two.inserted == 1 && one.duplicates + two.duplicates == 1);

    drop(stream);
    let lost_receipt = batch(vec![event(9_000)]);
    let denied = batch(vec![event(10_000)]);
    let (publisher, mut connection) = AsyncClient::new(harness.options(), 8);
    let events = tokio::spawn(async move {
        loop {
            connection.poll().await?;
        }
        #[allow(unreachable_code)]
        Ok::<(), rumqttc::ConnectionError>(())
    });
    publisher
        .publish(
            "devices/north-van/telemetry",
            QoS::AtLeastOnce,
            false,
            serde_json::to_vec(&lost_receipt)?,
        )
        .await?;
    publisher
        .publish(
            "devices/south-van/telemetry",
            QoS::AtLeastOnce,
            false,
            serde_json::to_vec(&denied)?,
        )
        .await?;
    let start = Instant::now();
    while count(&database, lost_receipt.events[0].event_id).await? == 0 {
        ensure!(
            start.elapsed() < Duration::from_secs(10),
            "published event never committed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ensure!(
        harness.accept(&lost_receipt).await?.duplicates == 1,
        "lost receipt replay duplicated data"
    );
    ensure!(
        count(&database, denied.events[0].event_id).await? == 0,
        "broker allowed cross-device publishing"
    );
    events.abort();

    check_queue(&harness, &database).await?;
    check_drain(&harness, &database).await?;
    sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(run.to_string())
        .execute(&database)
        .await?;
    println!(
        "PASS: HTTP, MQTT, tenant isolation, validation, atomic conflicts, concurrent retries, lost receipts, history, backpressure and shutdown drain"
    );
    Ok(())
}

async fn check_queue(harness: &Harness, database: &PgPool) -> anyhow::Result<()> {
    let mut lock = database.begin().await?;
    sqlx::query("SELECT id FROM devices WHERE id = 'north-van' FOR NO KEY UPDATE")
        .fetch_one(&mut *lock)
        .await?;

    let mut requests = FuturesUnordered::new();
    for _ in 0..96 {
        requests.push(async {
            let response = harness.post(NORTH, &batch(vec![event(11_000)])).await?;
            Ok::<StatusCode, anyhow::Error>(response.status())
        });
    }
    let first = tokio::time::timeout(Duration::from_secs(4), requests.next())
        .await?
        .context("no overload response")??;
    lock.rollback().await?;
    ensure!(
        first == StatusCode::TOO_MANY_REQUESTS,
        "expected 429 at saturation, got {first}"
    );
    while let Some(result) = requests.next().await {
        let status = result?;
        ensure!(
            matches!(status, StatusCode::OK | StatusCode::TOO_MANY_REQUESTS),
            "unexpected overload status {status}"
        );
    }
    Ok(())
}

async fn check_drain(harness: &Harness, database: &PgPool) -> anyhow::Result<()> {
    let store = Store::open(&harness.config.database, harness.config.mode, 2).await?;
    let device = store
        .by_id(NORTH_DEVICE)
        .await?
        .context("missing test device")?;
    let mut lock = database.begin().await?;
    sqlx::query("SELECT id FROM devices WHERE id = 'north-van' FOR NO KEY UPDATE")
        .fetch_one(&mut *lock)
        .await?;
    let (ingest, dispatcher) = Ingestion::start(store, Metrics::new()?, 3, 1);
    let first = batch(vec![event(12_000)]);
    let second = batch(vec![event(13_000)]);
    let (discard_reply, discard_receipt) = tokio::sync::oneshot::channel();
    drop(discard_receipt);
    let (reply, receipt) = tokio::sync::oneshot::channel();
    ingest.submit(
        Identity::Http(device.clone()),
        &serde_json::to_vec(&first)?,
        Reply::Http(discard_reply),
    );
    ingest.submit(
        Identity::Http(device.clone()),
        &serde_json::to_vec(&second)?,
        Reply::Http(reply),
    );
    ingest.stopping.cancel();
    let (reply, rejected) = tokio::sync::oneshot::channel();
    ingest.submit(
        Identity::Http(device),
        &serde_json::to_vec(&first)?,
        Reply::Http(reply),
    );
    ensure!(
        rejected.await?.outcome == Outcome::Unavailable,
        "shutdown continued admitting work"
    );
    lock.rollback().await?;
    tokio::time::timeout(Duration::from_secs(5), dispatcher).await??;
    ensure!(
        receipt.await?.outcome == Outcome::Committed,
        "accepted batch was not drained"
    );
    ensure!(
        count(database, first.events[0].event_id).await? == 1,
        "dropped receiver canceled persistence"
    );
    ensure!(
        count(database, second.events[0].event_id).await? == 1,
        "queued batch missing after drain"
    );
    Ok(())
}

async fn count(database: &PgPool, id: Uuid) -> anyhow::Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM telemetry WHERE event_id = $1")
            .bind(id)
            .fetch_one(database)
            .await?,
    )
}

fn event(observed_at_ms: i64) -> TelemetryEvent {
    TelemetryEvent {
        event_id: Uuid::new_v4(),
        observed_at_ms,
        latitude_e6: 12_971_599,
        longitude_e6: 77_594_566,
        speed_cm_s: 12,
    }
}

fn batch(events: Vec<TelemetryEvent>) -> Batch {
    Batch {
        batch_id: Uuid::new_v4(),
        events,
    }
}
