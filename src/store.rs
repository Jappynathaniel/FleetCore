use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use sqlx::{
    PgPool, Postgres, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use crate::protocol::Event;

mod binary;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum WriteMode {
    #[default]
    Unnest,
    Copy,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct Device {
    pub id: String,
    pub tenant_id: String,
}

#[derive(Clone)]
pub struct Store {
    pub pool: PgPool,
    mode: WriteMode,
}

#[derive(Debug)]
pub struct WriteResult {
    pub inserted: usize,
    pub database_time: Duration,
    pub commit_time: Duration,
}

#[derive(Debug)]
pub enum WriteError {
    Conflict,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for WriteError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl Store {
    pub async fn open(url: &str, mode: WriteMode, connections: u32) -> anyhow::Result<Self> {
        let options = PgConnectOptions::from_str(url)?;
        let pool = PgPoolOptions::new()
            .max_connections(connections)
            .acquire_timeout(Duration::from_secs(3))
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '10s'")
                        .execute(&mut *connection)
                        .await?;
                    sqlx::query("SET lock_timeout = '5s'")
                        .execute(&mut *connection)
                        .await?;
                    if mode == WriteMode::Copy {
                        sqlx::query(binary::CREATE_STAGE)
                            .execute(&mut *connection)
                            .await?;
                    }
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool, mode })
    }

    pub async fn seed_demo(&self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        for (tenant, operator, device, token) in [
            (
                "north",
                "north-operator-demo",
                "north-van",
                "north-van-demo",
            ),
            (
                "south",
                "south-operator-demo",
                "south-van",
                "south-van-demo",
            ),
        ] {
            sqlx::query("INSERT INTO tenants VALUES ($1, sha256(convert_to($2, 'UTF8'))) ON CONFLICT DO NOTHING")
                .bind(tenant).bind(operator).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO devices VALUES ($1, $2, sha256(convert_to($3, 'UTF8'))) ON CONFLICT DO NOTHING")
                .bind(device).bind(tenant).bind(token).execute(&mut *tx).await?;
        }
        tx.commit().await
    }

    pub async fn by_token(&self, token: &str) -> Result<Option<Device>, sqlx::Error> {
        sqlx::query_as(
            "SELECT id, tenant_id FROM devices WHERE token_hash = sha256(convert_to($1, 'UTF8'))",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn by_id(&self, id: &str) -> Result<Option<Device>, sqlx::Error> {
        sqlx::query_as("SELECT id, tenant_id FROM devices WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn operator(&self, token: &str) -> Result<Option<String>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT id FROM tenants WHERE operator_token_hash = sha256(convert_to($1, 'UTF8'))",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn page(
        &self,
        device: &Device,
        cursor: Option<(i64, Uuid)>,
        limit: i64,
    ) -> Result<Vec<Event>, sqlx::Error> {
        const FIELDS: &str = "event_id, observed_at_ms, latitude_e6, longitude_e6, speed_cm_s";
        if let Some((time, id)) = cursor {
            let statement = format!(
                "SELECT {FIELDS} FROM telemetry WHERE tenant_id = $1 AND device_id = $2
                 AND (observed_at_ms, event_id) < ($3, $4)
                 ORDER BY observed_at_ms DESC, event_id DESC LIMIT $5"
            );
            sqlx::query_as(&statement)
                .bind(&device.tenant_id)
                .bind(&device.id)
                .bind(time)
                .bind(id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
        } else {
            let statement = format!(
                "SELECT {FIELDS} FROM telemetry WHERE tenant_id = $1 AND device_id = $2
                 ORDER BY observed_at_ms DESC, event_id DESC LIMIT $3"
            );
            sqlx::query_as(&statement)
                .bind(&device.tenant_id)
                .bind(&device.id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
        }
    }

    pub async fn write(
        &self,
        device: &Device,
        events: &[Event],
    ) -> Result<WriteResult, WriteError> {
        let started = Instant::now();
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM devices WHERE tenant_id = $1 AND id = $2 FOR NO KEY UPDATE")
            .bind(&device.tenant_id)
            .bind(&device.id)
            .fetch_one(&mut *tx)
            .await?;
        let (inserted, matched) = match self.mode {
            WriteMode::Unnest => write_unnest(&mut tx, device, events).await?,
            WriteMode::Copy => binary::write(&mut tx, device, events).await?,
        };
        if matched != events.len() as i64 {
            tx.rollback().await?;
            return Err(WriteError::Conflict);
        }
        let database_time = started.elapsed();
        let commit_started = Instant::now();
        tx.commit().await?;
        Ok(WriteResult {
            inserted: inserted as usize,
            database_time,
            commit_time: commit_started.elapsed(),
        })
    }
}

const MERGE: &str = "
), inserted AS (
    INSERT INTO telemetry (tenant_id, device_id, event_id, observed_at_ms, latitude_e6, longitude_e6, speed_cm_s)
    SELECT $1, $2, event_id, observed_at_ms, latitude_e6, longitude_e6, speed_cm_s FROM incoming
    ON CONFLICT (tenant_id, device_id, event_id) DO NOTHING
    RETURNING event_id
)
SELECT (SELECT count(*) FROM inserted), (
    SELECT count(*) FROM incoming AS i
    LEFT JOIN inserted AS n ON n.event_id = i.event_id
    LEFT JOIN telemetry AS t ON t.tenant_id = $1 AND t.device_id = $2 AND t.event_id = i.event_id
    WHERE n.event_id IS NOT NULL
       OR (t.observed_at_ms, t.latitude_e6, t.longitude_e6, t.speed_cm_s)
            = (i.observed_at_ms, i.latitude_e6, i.longitude_e6, i.speed_cm_s)
)";

async fn write_unnest(
    tx: &mut Transaction<'_, Postgres>,
    device: &Device,
    events: &[Event],
) -> Result<(i64, i64), sqlx::Error> {
    let ids: Vec<_> = events.iter().map(|e| e.event_id).collect();
    let times: Vec<_> = events.iter().map(|e| e.observed_at_ms).collect();
    let latitudes: Vec<_> = events.iter().map(|e| e.latitude_e6).collect();
    let longitudes: Vec<_> = events.iter().map(|e| e.longitude_e6).collect();
    let speeds: Vec<_> = events.iter().map(|e| e.speed_cm_s).collect();
    let statement = format!(
        "WITH incoming AS (
           SELECT * FROM unnest($3::uuid[], $4::bigint[], $5::integer[], $6::integer[], $7::integer[])
                AS input(event_id, observed_at_ms, latitude_e6, longitude_e6, speed_cm_s)
         {MERGE}"
    );
    sqlx::query_as(&statement)
        .bind(&device.tenant_id)
        .bind(&device.id)
        .bind(ids)
        .bind(times)
        .bind(latitudes)
        .bind(longitudes)
        .bind(speeds)
        .fetch_one(&mut **tx)
        .await
}
