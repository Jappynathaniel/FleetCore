use sqlx::{Postgres, Transaction};

use super::{Device, MERGE};
use crate::protocol::Event;

pub(super) const CREATE_STAGE: &str = "
    CREATE TEMP TABLE fleetcore_incoming (
        event_id uuid NOT NULL,
        observed_at_ms bigint NOT NULL,
        latitude_e6 integer NOT NULL,
        longitude_e6 integer NOT NULL,
        speed_cm_s integer NOT NULL
    ) ON COMMIT DELETE ROWS";

pub(super) async fn write(
    tx: &mut Transaction<'_, Postgres>,
    device: &Device,
    events: &[Event],
) -> Result<(i64, i64), sqlx::Error> {
    let payload = encode(events);
    let mut copy = tx
        .copy_in_raw(
            "COPY pg_temp.fleetcore_incoming
         (event_id, observed_at_ms, latitude_e6, longitude_e6, speed_cm_s)
         FROM STDIN (FORMAT BINARY)",
        )
        .await?;
    if let Err(error) = copy.send(payload).await {
        let _ = copy.abort("sending FleetCore COPY data failed").await;
        return Err(error);
    }
    let copied = copy.finish().await?;
    if copied != events.len() as u64 {
        return Err(sqlx::Error::Protocol(format!(
            "staging received {copied} rows for a batch of {}",
            events.len()
        )));
    }

    let statement = format!(
        "WITH incoming AS (
            SELECT event_id, observed_at_ms, latitude_e6, longitude_e6, speed_cm_s
            FROM pg_temp.fleetcore_incoming
         {MERGE}"
    );
    sqlx::query_as(&statement)
        .bind(&device.tenant_id)
        .bind(&device.id)
        .fetch_one(&mut **tx)
        .await
}

fn encode(events: &[Event]) -> Vec<u8> {
    // A binary COPY stream has a signature, zero flags/extensions, fixed-width
    // fields with length prefixes, and a final row marker of -1.
    const ROW_BYTES: usize = 2 + 5 * 4 + 16 + 8 + 3 * 4;
    let mut payload = Vec::with_capacity(21 + events.len() * ROW_BYTES);
    payload.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    payload.extend_from_slice(&[0; 8]);
    for event in events {
        payload.extend_from_slice(&5_i16.to_be_bytes());
        field(&mut payload, event.event_id.as_bytes());
        field(&mut payload, &event.observed_at_ms.to_be_bytes());
        field(&mut payload, &event.latitude_e6.to_be_bytes());
        field(&mut payload, &event.longitude_e6.to_be_bytes());
        field(&mut payload, &event.speed_cm_s.to_be_bytes());
    }
    payload.extend_from_slice(&(-1_i16).to_be_bytes());
    payload
}

fn field(payload: &mut Vec<u8>, bytes: &[u8]) {
    payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    payload.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn encodes_a_signed_coordinate_and_uses_network_byte_order() {
        let event = Event {
            event_id: Uuid::from_u128(1),
            observed_at_ms: 1_700_000_000_000,
            latitude_e6: -654_321,
            longitude_e6: 12_345,
            speed_cm_s: 2_999,
        };
        let wire = encode(std::slice::from_ref(&event));
        assert_eq!(&wire[..19], b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0");
        assert_eq!(&wire[19..25], &[0, 5, 0, 0, 0, 16]);
        assert_eq!(&wire[25..41], event.event_id.as_bytes());
        assert_eq!(&wire[41..45], &[0, 0, 0, 8]);
        assert_eq!(&wire[45..53], &event.observed_at_ms.to_be_bytes());
        assert_eq!(&wire[53..57], &[0, 0, 0, 4]);
        assert_eq!(&wire[57..61], &event.latitude_e6.to_be_bytes());
        assert_eq!(&wire[77..], &[0xff, 0xff]);
    }
}
