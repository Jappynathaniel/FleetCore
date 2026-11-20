use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, SubscribeReasonCode};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    admission::{AdmissionContext, TenantAdmissionLayer, bearer, retry_response as retry},
    protocol::{Event as TelemetryEvent, MAX_BODY, Outcome},
    service::{App, Identity, Ingestion, Reply},
    store::Device,
};

pub fn router(app: App) -> Router {
    let ingest = Router::new()
        .route("/v1/telemetry/batches", post(ingest_http))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .route_layer(TenantAdmissionLayer {
            store: app.store.clone(),
            admission: app.ingestion.admission.clone(),
            requests: app.http_admission.clone(),
            stopping: app.ingestion.stopping.clone(),
            metrics: app.ingestion.metrics.clone(),
            body_timeout: app.body_timeout,
        });
    Router::new()
        .merge(ingest)
        .route("/v1/vehicles/{device}/latest", get(latest))
        .route("/v1/vehicles/{device}/telemetry", get(history))
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(app)
}

async fn ingest_http(
    State(app): State<App>,
    Extension(device): Extension<Device>,
    Extension(context): Extension<AdmissionContext>,
    body: Bytes,
) -> Response {
    let (send, receive) = oneshot::channel();
    app.ingestion.submit_with_lease(
        Identity::Http(device),
        &body,
        Reply::Http(send),
        context.lease,
    );
    let receipt = match tokio::time::timeout(Duration::from_secs(30), receive).await {
        Ok(Ok(receipt)) => receipt,
        _ => return retry(StatusCode::SERVICE_UNAVAILABLE),
    };
    let status = match receipt.outcome {
        Outcome::Committed => StatusCode::OK,
        Outcome::Invalid => StatusCode::BAD_REQUEST,
        Outcome::Conflict => StatusCode::CONFLICT,
        Outcome::Busy => StatusCode::TOO_MANY_REQUESTS,
        Outcome::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut response = (status, Json(receipt)).into_response();
    if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, "1".parse().expect("valid header"));
    }
    response
}

fn query_error(error: sqlx::Error) -> StatusCode {
    tracing::warn!(%error, "database query failed");
    StatusCode::SERVICE_UNAVAILABLE
}

async fn read_identity(app: &App, headers: &HeaderMap, device: &str) -> Result<Device, StatusCode> {
    let token = bearer(headers)?;
    let tenant = app
        .store
        .operator(token)
        .await
        .map_err(query_error)?
        .ok_or(StatusCode::UNAUTHORIZED)?;
    app.store
        .by_id(device)
        .await
        .map_err(query_error)?
        .filter(|device| device.tenant_id == tenant)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn latest(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let device = read_identity(&app, &headers, &id).await?;
    let mut events = app
        .store
        .page(&device, None, 1)
        .await
        .map_err(query_error)?;
    Ok(match events.pop() {
        Some(event) => Json(event).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

#[derive(Deserialize)]
struct HistoryQuery {
    cursor: Option<String>,
    limit: Option<i64>,
}

#[derive(Serialize)]
struct History {
    events: Vec<TelemetryEvent>,
    next_cursor: Option<String>,
}

fn parse_cursor(cursor: &str) -> Option<(i64, Uuid)> {
    let (time, id) = cursor.split_once(':')?;
    Some((time.parse().ok()?, Uuid::parse_str(id).ok()?))
}

async fn history(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<History>, StatusCode> {
    let device = read_identity(&app, &headers, &id).await?;
    let limit = query.limit.unwrap_or(100);
    if !(1..=500).contains(&limit) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let cursor = match query.cursor {
        Some(cursor) => Some(parse_cursor(&cursor).ok_or(StatusCode::BAD_REQUEST)?),
        None => None,
    };
    let mut events = app
        .store
        .page(&device, cursor, limit + 1)
        .await
        .map_err(query_error)?;
    let next_cursor = if events.len() > limit as usize {
        events.pop();
        events
            .last()
            .map(|event| format!("{}:{}", event.observed_at_ms, event.event_id))
    } else {
        None
    };
    Ok(Json(History {
        events,
        next_cursor,
    }))
}

async fn ready(State(app): State<App>) -> StatusCode {
    if app.ready().await {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics(State(app): State<App>) -> Result<impl IntoResponse, StatusCode> {
    let body = app
        .ingestion
        .metrics
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    ))
}

pub fn mqtt_options(
    id: String,
    host: String,
    port: u16,
    username: String,
    password: String,
) -> MqttOptions {
    let mut options = MqttOptions::new(id, host, port);
    options.set_credentials(username, password);
    options.set_keep_alive(Duration::from_secs(15));
    options.set_max_packet_size(MAX_BODY + 1024, MAX_BODY + 1024);
    options
}

pub async fn run_mqtt(
    options: MqttOptions,
    ingestion: Ingestion,
    ready: Arc<AtomicBool>,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let (client, mut event_loop) = AsyncClient::new(options, 128);
    let (send, mut receive) = mpsc::channel::<(String, crate::protocol::Receipt)>(128);
    let publisher = client.clone();
    let publish_metrics = ingestion.metrics.clone();
    let publish_task = tokio::spawn(async move {
        while let Some((device, receipt)) = receive.recv().await {
            if let Ok(payload) = serde_json::to_vec(&receipt) {
                let result = publisher.try_publish(
                    format!("devices/{device}/receipts"),
                    QoS::AtLeastOnce,
                    false,
                    payload,
                );
                if result.is_err() {
                    publish_metrics
                        .outcomes
                        .with_label_values(&["mqtt", "receipt_dropped"])
                        .inc();
                }
            }
        }
    });

    loop {
        let event = tokio::select! {
            _ = stop.cancelled() => break,
            event = event_loop.poll() => event,
        };
        match event {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                let subscriber = client.clone();
                tokio::spawn(async move {
                    if let Err(error) = subscriber
                        .subscribe("devices/+/telemetry", QoS::AtLeastOnce)
                        .await
                    {
                        tracing::warn!(%error, "could not queue MQTT subscription");
                    }
                });
            }
            Ok(Event::Incoming(Packet::SubAck(ack))) => {
                let accepted = matches!(
                    ack.return_codes.as_slice(),
                    [SubscribeReasonCode::Success(_)]
                );
                ready.store(accepted, Ordering::Relaxed);
                if !accepted {
                    tracing::error!("broker rejected the service subscription");
                }
            }
            Ok(Event::Incoming(Packet::Publish(packet))) if !packet.retain => {
                if let Some(device) = packet
                    .topic
                    .strip_prefix("devices/")
                    .and_then(|t| t.strip_suffix("/telemetry"))
                    && !device.is_empty()
                    && device.len() <= 64
                    && device.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
                {
                    ingestion.submit(
                        Identity::Mqtt(device.to_owned()),
                        &packet.payload,
                        Reply::Mqtt(send.clone()),
                    );
                }
            }
            Ok(_) => {}
            Err(error) => {
                ready.store(false, Ordering::Relaxed);
                tracing::warn!(%error, "MQTT transport disconnected");
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
    ready.store(false, Ordering::Relaxed);
    drop(send);
    publish_task.abort();
    Ok(())
}
