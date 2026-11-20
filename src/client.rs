use std::time::Duration;

use anyhow::Context;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, SubscribeReasonCode};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::protocol::{Batch, Outcome, Receipt};

pub enum Delivery {
    Acknowledged(Receipt),
    Retry(Duration),
    Rejected(String),
}

pub async fn send_http(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    batch: &Batch,
) -> anyhow::Result<Delivery> {
    let response = client
        .post(format!("{base}/v1/telemetry/batches"))
        .bearer_auth(token)
        .json(batch)
        .send()
        .await?;
    let status = response.status();
    if status.as_u16() == 429 || status.as_u16() == 408 || status.is_server_error() {
        return Ok(Delivery::Retry(server_retry_delay(response.headers())));
    }
    if !status.is_success() {
        return Ok(Delivery::Rejected(format!("HTTP {status}")));
    }
    let receipt = response.json::<Receipt>().await?;
    if receipt.batch_id != batch.batch_id || receipt.outcome != Outcome::Committed {
        return Ok(Delivery::Retry(Duration::ZERO));
    }
    Ok(Delivery::Acknowledged(receipt))
}

pub struct MqttSender {
    client: AsyncClient,
    ready: watch::Receiver<bool>,
    receipts: mpsc::Receiver<Receipt>,
    topic: String,
    event_task: JoinHandle<()>,
}

impl MqttSender {
    pub fn new(options: MqttOptions, device: &str) -> Self {
        let (client, mut event_loop) = AsyncClient::new(options, 32);
        let subscription = format!("devices/{device}/receipts");
        let (receipt_sender, receipts) = mpsc::channel(32);
        let (state, ready) = watch::channel(false);
        let subscriber = client.clone();
        let event_task = tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        if let Err(error) =
                            subscriber.try_subscribe(&subscription, QoS::AtLeastOnce)
                        {
                            tracing::warn!(%error, "could not queue MQTT subscription");
                        }
                    }
                    Ok(Event::Incoming(Packet::SubAck(ack))) => {
                        let accepted = matches!(
                            ack.return_codes.as_slice(),
                            [SubscribeReasonCode::Success(_)]
                        );
                        let _ = state.send(accepted);
                    }
                    Ok(Event::Incoming(Packet::Publish(packet)))
                        if packet.topic == subscription && !packet.retain =>
                    {
                        if let Ok(receipt) = serde_json::from_slice(&packet.payload) {
                            let _ = receipt_sender.try_send(receipt);
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let _ = state.send(false);
                        tracing::debug!(%error, "simulator MQTT connection unavailable");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
        Self {
            client,
            ready,
            receipts,
            topic: format!("devices/{device}/telemetry"),
            event_task,
        }
    }

    pub async fn send(&mut self, batch: &Batch) -> anyhow::Result<Delivery> {
        let timeout = tokio::time::timeout(Duration::from_secs(5), async {
            self.ready.wait_for(|ready| *ready).await.map(|_| ())
        })
        .await;
        match timeout {
            Ok(ready) => {
                ready.context("MQTT publisher stopped")?;
            }
            Err(_) => return Ok(Delivery::Retry(Duration::ZERO)),
        }
        let result = tokio::time::timeout(Duration::from_secs(30), self.send_and_wait(batch)).await;
        match result {
            Ok(result) => result,
            Err(_) => Ok(Delivery::Retry(Duration::ZERO)),
        }
    }

    async fn send_and_wait(&mut self, batch: &Batch) -> anyhow::Result<Delivery> {
        self.client
            .publish(
                &self.topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(batch)?,
            )
            .await?;
        loop {
            let receipt = self
                .receipts
                .recv()
                .await
                .context("MQTT publisher stopped")?;
            if receipt.batch_id != batch.batch_id {
                continue;
            }
            return Ok(match receipt.outcome {
                Outcome::Committed => Delivery::Acknowledged(receipt),
                Outcome::Busy | Outcome::Unavailable => Delivery::Retry(Duration::ZERO),
                outcome => Delivery::Rejected(format!("{outcome:?}")),
            });
        }
    }
}

impl Drop for MqttSender {
    fn drop(&mut self) {
        self.event_task.abort();
    }
}

pub fn retry_delay(attempt: usize) -> Duration {
    let cap = 200_u64 * (1 << attempt.min(5));
    Duration::from_millis(rand::random_range(cap / 2..=cap))
}

fn server_retry_delay(headers: &reqwest::header::HeaderMap) -> Duration {
    let Some(value) = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
    else {
        return Duration::ZERO;
    };
    if let Ok(seconds) = value.parse::<u64>() {
        return Duration::from_secs(seconds);
    }
    httpdate::parse_http_date(value)
        .ok()
        .and_then(|deadline| deadline.duration_since(std::time::SystemTime::now()).ok())
        .unwrap_or_default()
}

pub fn retry_delay_after(attempt: usize, minimum: Duration) -> Duration {
    minimum.saturating_add(retry_delay(attempt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_retry_minimum_is_preserved_with_jitter() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "2".parse().unwrap());
        let minimum = server_retry_delay(&headers);
        for attempt in 0..12 {
            let delay = retry_delay_after(attempt, minimum);
            assert!(delay >= Duration::from_secs(2));
            assert!(delay <= Duration::from_millis(8400));
        }
        headers.insert(reqwest::header::RETRY_AFTER, "bad".parse().unwrap());
        assert_eq!(server_retry_delay(&headers), Duration::ZERO);
        let deadline = std::time::SystemTime::now() + Duration::from_secs(10);
        headers.insert(
            reqwest::header::RETRY_AFTER,
            httpdate::fmt_http_date(deadline).parse().unwrap(),
        );
        assert!(server_retry_delay(&headers) >= Duration::from_secs(8));
    }
}
