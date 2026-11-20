use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    admission::{Admission, AdmissionLease},
    metrics::Metrics,
    protocol::{Batch, MAX_BODY, Outcome, Receipt},
    store::{Device, Store, WriteError},
};

pub enum Identity {
    Http(Device),
    Mqtt(String),
    MqttResolved(Device),
}

impl Identity {
    fn transport(&self) -> &'static str {
        match self {
            Self::Http(_) => "http",
            Self::Mqtt(_) | Self::MqttResolved(_) => "mqtt",
        }
    }
}

pub enum Reply {
    Http(oneshot::Sender<Receipt>),
    Mqtt(mpsc::Sender<(String, Receipt)>),
}

impl Reply {
    fn send(self, device: &str, receipt: Receipt) {
        match self {
            Self::Http(sender) => {
                let _ = sender.send(receipt);
            }
            Self::Mqtt(sender) => {
                if sender.try_send((device.to_owned(), receipt)).is_err() {
                    tracing::debug!("MQTT receipt queue unavailable, client can replay");
                }
            }
        }
    }
}

struct Job {
    identity: Identity,
    batch: Batch,
    reply: Reply,
    queued_at: Instant,
    lease: Option<AdmissionLease>,
}

#[derive(Clone)]
pub struct Ingestion {
    store: Store,
    lookups: Arc<Semaphore>,
    queue: mpsc::Sender<Job>,
    pub metrics: Metrics,
    pub stopping: CancellationToken,
    pub admission: Admission,
}

impl Ingestion {
    pub fn start(
        store: Store,
        metrics: Metrics,
        queue_capacity: usize,
        workers: usize,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let admission = Admission::new(false, 16, 8, &metrics);
        Self::start_with_admission(store, metrics, queue_capacity, workers, admission)
    }

    pub fn start_with_admission(
        store: Store,
        metrics: Metrics,
        queue_capacity: usize,
        workers: usize,
        admission: Admission,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (queue, mut jobs) = mpsc::channel::<Job>(queue_capacity);
        let stopping = CancellationToken::new();
        let stop = stopping.clone();
        let ingestion = Self {
            store: store.clone(),
            lookups: Arc::new(Semaphore::new(workers)),
            queue,
            metrics: metrics.clone(),
            stopping,
            admission: admission.clone(),
        };
        let dispatcher = tokio::spawn(async move {
            let permits = Arc::new(Semaphore::new(workers));
            let mut tasks = tokio::task::JoinSet::new();
            let mut draining = false;
            loop {
                let permit = tokio::select! {
                    biased;
                    _ = stop.cancelled(), if !draining => {
                        jobs.close();
                        draining = true;
                        continue;
                    }
                    Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                        if let Err(error) = result {
                            tracing::error!(%error, "ingestion worker failed");
                        }
                        continue;
                    }
                    permit = permits.clone().acquire_owned() => permit.expect("worker semaphore is never closed"),
                };
                let job = tokio::select! {
                    biased;
                    _ = stop.cancelled(), if !draining => {
                        jobs.close();
                        draining = true;
                        continue;
                    }
                    job = jobs.recv() => job,
                };
                let Some(job) = job else { break };
                metrics.queue_depth.dec();
                let store = store.clone();
                let metrics = metrics.clone();
                let admission = admission.clone();
                tasks.spawn(async move {
                    process(&store, &metrics, &admission, job).await;
                    drop(permit);
                });
            }
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    tracing::error!(%error, "ingestion worker failed");
                }
            }
        });
        (ingestion, dispatcher)
    }

    pub fn submit(&self, identity: Identity, body: &[u8], reply: Reply) {
        if self.admission.enabled()
            && let Identity::Mqtt(id) = identity
        {
            let Ok(permit) = self.lookups.clone().try_acquire_owned() else {
                // No receipt is safe: the durable sender retries unconfirmed work.
                self.metrics
                    .rejections
                    .with_label_values(&["mqtt", "lookup_busy"])
                    .inc();
                return;
            };
            let ingestion = self.clone();
            let body = body.to_vec();
            tokio::spawn(async move {
                let identity = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    ingestion.store.by_id(&id),
                )
                .await;
                if let Ok(Ok(Some(device))) = identity {
                    match ingestion.admission.try_acquire(&device.tenant_id) {
                        Ok(lease) => ingestion.submit_with_lease(
                            Identity::MqttResolved(device),
                            &body,
                            reply,
                            lease,
                        ),
                        Err(reason) => {
                            ingestion
                                .metrics
                                .rejections
                                .with_label_values(&["mqtt", reason])
                                .inc();
                            let batch_id = serde_json::from_slice::<Batch>(&body)
                                .map(|batch| batch.batch_id)
                                .unwrap_or_default();
                            reply.send(&id, Receipt::rejected(batch_id, Outcome::Busy));
                        }
                    }
                }
                drop(permit);
            });
            return;
        }
        self.submit_with_lease(identity, body, reply, None);
    }

    pub fn submit_with_lease(
        &self,
        identity: Identity,
        body: &[u8],
        reply: Reply,
        lease: Option<AdmissionLease>,
    ) {
        let transport = identity.transport();
        let started = Instant::now();
        let decoded = if body.len() <= MAX_BODY {
            serde_json::from_slice::<Batch>(body).ok()
        } else {
            None
        };
        let Some(mut batch) = decoded else {
            self.metrics
                .stage
                .with_label_values(&[transport, "validation"])
                .observe(started.elapsed().as_secs_f64());
            self.metrics
                .outcomes
                .with_label_values(&[transport, "invalid"])
                .inc();
            reply.send(
                device_id(&identity),
                Receipt::rejected(uuid::Uuid::nil(), Outcome::Invalid),
            );
            return;
        };
        let validation = batch.validate();
        self.metrics
            .stage
            .with_label_values(&[transport, "validation"])
            .observe(started.elapsed().as_secs_f64());
        let rejection = match validation {
            Err(outcome) => outcome,
            Ok(()) if self.stopping.is_cancelled() => Outcome::Unavailable,
            Ok(()) => match self.queue.try_reserve() {
                Ok(slot) => {
                    self.metrics.queue_depth.inc();
                    slot.send(Job {
                        identity,
                        batch,
                        reply,
                        queued_at: Instant::now(),
                        lease,
                    });
                    return;
                }
                Err(mpsc::error::TrySendError::Full(_)) => Outcome::Busy,
                Err(mpsc::error::TrySendError::Closed(_)) => Outcome::Unavailable,
            },
        };
        self.metrics
            .outcomes
            .with_label_values(&[transport, label(rejection)])
            .inc();
        reply.send(
            device_id(&identity),
            Receipt::rejected(batch.batch_id, rejection),
        );
    }
}

fn device_id(identity: &Identity) -> &str {
    match identity {
        Identity::Http(device) | Identity::MqttResolved(device) => &device.id,
        Identity::Mqtt(device) => device,
    }
}

async fn process(store: &Store, metrics: &Metrics, admission: &Admission, job: Job) {
    let mut lease = job.lease;
    let transport = job.identity.transport();
    let id = device_id(&job.identity);
    metrics
        .stage
        .with_label_values(&[transport, "queue"])
        .observe(job.queued_at.elapsed().as_secs_f64());
    let receipt = match &job.identity {
        Identity::Http(device) | Identity::MqttResolved(device) => {
            admitted_persist(
                store, metrics, admission, transport, device, &job.batch, &mut lease,
            )
            .await
        }
        Identity::Mqtt(id) => match store.by_id(id).await {
            Ok(Some(device)) => {
                admitted_persist(
                    store, metrics, admission, transport, &device, &job.batch, &mut lease,
                )
                .await
            }
            Ok(None) => Receipt::rejected(job.batch.batch_id, Outcome::Invalid),
            Err(error) => {
                tracing::warn!(%error, "device lookup unavailable");
                Receipt::rejected(job.batch.batch_id, Outcome::Unavailable)
            }
        },
    };
    metrics
        .outcomes
        .with_label_values(&[transport, label(receipt.outcome)])
        .inc();
    let handoff = Instant::now();
    job.reply.send(id, receipt);
    drop(lease);
    metrics
        .stage
        .with_label_values(&[transport, "receipt_handoff"])
        .observe(handoff.elapsed().as_secs_f64());
}

async fn admitted_persist(
    store: &Store,
    metrics: &Metrics,
    admission: &Admission,
    transport: &str,
    device: &Device,
    batch: &Batch,
    lease: &mut Option<AdmissionLease>,
) -> Receipt {
    if lease.is_none() {
        match admission.try_acquire(&device.tenant_id) {
            Ok(acquired) => *lease = acquired,
            Err(reason) => {
                metrics
                    .rejections
                    .with_label_values(&[transport, reason])
                    .inc();
                return Receipt::rejected(batch.batch_id, Outcome::Busy);
            }
        }
    }
    persist(store, metrics, transport, device, batch).await
}

async fn persist(
    store: &Store,
    metrics: &Metrics,
    transport: &str,
    device: &Device,
    batch: &Batch,
) -> Receipt {
    match store.write(device, &batch.events).await {
        Ok(result) => {
            metrics
                .stage
                .with_label_values(&[transport, "database"])
                .observe(result.database_time.as_secs_f64());
            metrics
                .stage
                .with_label_values(&[transport, "commit"])
                .observe(result.commit_time.as_secs_f64());
            Receipt {
                batch_id: batch.batch_id,
                outcome: Outcome::Committed,
                inserted: result.inserted,
                duplicates: batch.events.len() - result.inserted,
            }
        }
        Err(WriteError::Conflict) => Receipt::rejected(batch.batch_id, Outcome::Conflict),
        Err(WriteError::Database(error)) => {
            tracing::warn!(%error, "ingestion transaction unavailable");
            Receipt::rejected(batch.batch_id, Outcome::Unavailable)
        }
    }
}

fn label(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Committed => "committed",
        Outcome::Invalid => "invalid",
        Outcome::Conflict => "conflict",
        Outcome::Busy => "busy",
        Outcome::Unavailable => "unavailable",
    }
}

#[derive(Clone)]
pub struct App {
    pub store: Store,
    pub ingestion: Ingestion,
    pub http_admission: Arc<Semaphore>,
    pub mqtt_ready: Arc<AtomicBool>,
    pub body_timeout: std::time::Duration,
}

impl App {
    pub async fn ready(&self) -> bool {
        !self.ingestion.stopping.is_cancelled()
            && self.mqtt_ready.load(Ordering::Relaxed)
            && sqlx::query("SELECT 1")
                .execute(&self.store.pool)
                .await
                .is_ok()
    }
}
