CREATE TABLE tenants (
    id text PRIMARY KEY,
    operator_token_hash bytea NOT NULL UNIQUE
);

CREATE TABLE devices (
    id text PRIMARY KEY CHECK (id ~ '^[a-z0-9-]{1,64}$'),
    tenant_id text NOT NULL REFERENCES tenants,
    token_hash bytea NOT NULL UNIQUE,
    UNIQUE (tenant_id, id)
);

CREATE TABLE telemetry (
    tenant_id text NOT NULL,
    device_id text NOT NULL,
    event_id uuid NOT NULL,
    observed_at_ms bigint NOT NULL CHECK (observed_at_ms BETWEEN 0 AND 253402300799999),
    latitude_e6 integer NOT NULL CHECK (latitude_e6 BETWEEN -90000000 AND 90000000),
    longitude_e6 integer NOT NULL CHECK (longitude_e6 BETWEEN -180000000 AND 180000000),
    speed_cm_s integer NOT NULL CHECK (speed_cm_s BETWEEN 0 AND 20000),
    PRIMARY KEY (tenant_id, device_id, event_id),
    FOREIGN KEY (tenant_id, device_id) REFERENCES devices (tenant_id, id)
);

CREATE INDEX telemetry_history
    ON telemetry (tenant_id, device_id, observed_at_ms DESC, event_id DESC);
