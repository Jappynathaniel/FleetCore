use std::{
    collections::HashMap,
    error::Error as _,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::future::BoxFuture;
use prometheus::IntGauge;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};

use crate::{metrics::Metrics, protocol::MAX_BODY, store::Store};

#[derive(Clone)]
pub struct Admission {
    state: Arc<AdmissionState>,
}

struct AdmissionState {
    enabled: bool,
    global: Arc<Semaphore>,
    tenants: Mutex<HashMap<String, Weak<Semaphore>>>,
    tenant_limit: usize,
    active: IntGauge,
}

#[derive(Clone)]
pub struct AdmissionLease {
    _permits: Arc<WorkPermits>,
}

struct WorkPermits {
    _global: OwnedSemaphorePermit,
    _tenant: OwnedSemaphorePermit,
    active: IntGauge,
}

impl Drop for WorkPermits {
    fn drop(&mut self) {
        self.active.dec();
    }
}

impl Admission {
    pub fn enabled(&self) -> bool {
        self.state.enabled
    }

    pub fn new(enabled: bool, global: usize, tenant: usize, metrics: &Metrics) -> Self {
        Self {
            state: Arc::new(AdmissionState {
                enabled,
                global: Arc::new(Semaphore::new(global)),
                tenants: Mutex::new(HashMap::new()),
                tenant_limit: tenant,
                active: metrics.admitted.clone(),
            }),
        }
    }

    pub fn try_acquire(&self, tenant: &str) -> Result<Option<AdmissionLease>, &'static str> {
        if !self.state.enabled {
            return Ok(None);
        }
        let semaphore = {
            let mut tenants = self.state.tenants.lock().expect("admission mutex poisoned");
            // Inactive tenants leave weak entries, keeping the map bounded by live work.
            tenants.retain(|_, semaphore| semaphore.strong_count() > 0);
            match tenants.get(tenant).and_then(Weak::upgrade) {
                Some(semaphore) => semaphore,
                None => {
                    let semaphore = Arc::new(Semaphore::new(self.state.tenant_limit));
                    tenants.insert(tenant.to_owned(), Arc::downgrade(&semaphore));
                    semaphore
                }
            }
        };
        let tenant_permit = semaphore.try_acquire_owned().map_err(|_| "tenant_busy")?;
        let global_permit = self
            .state
            .global
            .clone()
            .try_acquire_owned()
            .map_err(|_| "global_busy")?;
        self.state.active.inc();
        Ok(Some(AdmissionLease {
            _permits: Arc::new(WorkPermits {
                _global: global_permit,
                _tenant: tenant_permit,
                active: self.state.active.clone(),
            }),
        }))
    }
}

#[derive(Clone)]
pub struct AdmissionContext {
    pub lease: Option<AdmissionLease>,
}

#[derive(Clone)]
pub struct TenantAdmissionLayer {
    pub store: Store,
    pub admission: Admission,
    pub requests: Arc<Semaphore>,
    pub stopping: CancellationToken,
    pub metrics: Metrics,
    pub body_timeout: Duration,
}

impl<S> Layer<S> for TenantAdmissionLayer {
    type Service = TenantAdmissionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TenantAdmissionService {
            inner,
            layer: self.clone(),
        }
    }
}

#[derive(Clone)]
pub struct TenantAdmissionService<S> {
    inner: S,
    layer: TenantAdmissionLayer,
}

impl<S> Service<Request<Body>> for TenantAdmissionService<S>
where
    S: Service<Request<Body>, Response = Response> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: Send,
{
    type Response = Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Response, Self::Error>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let replacement = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, replacement);
        let layer = self.layer.clone();
        Box::pin(async move {
            if layer.stopping.is_cancelled() {
                return Ok(retry_response(StatusCode::SERVICE_UNAVAILABLE));
            }
            let Ok(request_permit) = layer.requests.clone().try_acquire_owned() else {
                layer
                    .metrics
                    .outcomes
                    .with_label_values(&["http", "request_busy"])
                    .inc();
                return Ok(retry_response(StatusCode::TOO_MANY_REQUESTS));
            };
            let token = match bearer(request.headers()) {
                Ok(token) => token,
                Err(status) => return Ok(status.into_response()),
            };
            let started = Instant::now();
            let identity =
                tokio::time::timeout(Duration::from_secs(2), layer.store.by_token(token)).await;
            layer
                .metrics
                .stage
                .with_label_values(&["http", "authentication"])
                .observe(started.elapsed().as_secs_f64());
            let device = match identity {
                Ok(Ok(Some(device))) => device,
                Ok(Ok(None)) => return Ok(StatusCode::UNAUTHORIZED.into_response()),
                _ => return Ok(retry_response(StatusCode::SERVICE_UNAVAILABLE)),
            };
            let lease = match layer.admission.try_acquire(&device.tenant_id) {
                Ok(lease) => lease,
                Err(reason) => {
                    layer
                        .metrics
                        .rejections
                        .with_label_values(&["http", reason])
                        .inc();
                    layer
                        .metrics
                        .outcomes
                        .with_label_values(&["http", "busy"])
                        .inc();
                    return Ok(retry_response(StatusCode::TOO_MANY_REQUESTS));
                }
            };
            let (mut parts, body) = request.into_parts();
            let body =
                match tokio::time::timeout(layer.body_timeout, to_bytes(body, MAX_BODY)).await {
                    Ok(Ok(body)) => body,
                    Ok(Err(error)) => {
                        let status = if error
                            .source()
                            .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
                        {
                            StatusCode::PAYLOAD_TOO_LARGE
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        return Ok(status.into_response());
                    }
                    Err(_) => return Ok(retry_response(StatusCode::REQUEST_TIMEOUT)),
                };
            parts.extensions.insert(device);
            parts.extensions.insert(AdmissionContext { lease });
            drop(request_permit);
            inner
                .call(Request::from_parts(parts, Body::from(body)))
                .await
        })
    }
}

pub(crate) fn bearer(headers: &HeaderMap) -> Result<&str, StatusCode> {
    let (scheme, token) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || token.len() > 256 {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(token)
}

pub(crate) fn retry_response(status: StatusCode) -> Response {
    (status, [(header::RETRY_AFTER, "1")]).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_isolation_and_cloned_guards_preserve_the_work_budget() {
        let metrics = Metrics::new().unwrap();
        let admission = Admission::new(true, 4, 2, &metrics);
        let first = admission.try_acquire("north").unwrap().unwrap();
        let second = admission.try_acquire("north").unwrap().unwrap();
        assert!(matches!(admission.try_acquire("north"), Err("tenant_busy")));
        let other = admission.try_acquire("south").unwrap().unwrap();
        let clone = first.clone();
        drop(first);
        assert!(matches!(admission.try_acquire("north"), Err("tenant_busy")));
        drop(clone);
        assert!(admission.try_acquire("north").is_ok());
        drop(second);
        drop(other);
        assert_eq!(metrics.admitted.get(), 0);
        assert!(admission.state.tenants.lock().unwrap().len() <= 2);
    }
}
