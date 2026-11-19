"""Exercise distinct device connections using only the Python standard library."""
import asyncio
import json
import math
import random
import os
import struct
import time
import sys
import uuid

HOST = os.getenv("MQTT_HOST", "benchmark-broker")
COUNT = int(os.getenv("MQTT_CLIENTS", "100"))
HOLD = int(os.getenv("MQTT_HOLD_SECONDS", "30"))
ATTEMPTS = int(os.getenv("MQTT_ATTEMPTS", "20"))
JITTER = os.getenv("MQTT_JITTER", "1") == "1"
if not 1 <= ATTEMPTS <= 32:
    raise SystemExit("Use 1–32 attempts")
if not 1 <= COUNT <= 1000 or not 0 <= HOLD <= 30:
    raise SystemExit("Use 1–1000 clients and 0–30 idle hold seconds")
SPECS = [{"batch_id": str(uuid.uuid4()), "events": [{
    "event_id": str(uuid.uuid4()), "observed_at_ms": 1800000000000,
    "latitude_e6": index, "longitude_e6": -index, "speed_cm_s": 123
}]} for index in range(COUNT)]

def string(value):
    data = value.encode()
    return struct.pack("!H", len(data)) + data

def packet(kind, data):
    length = len(data)
    encoded = bytearray()
    while True:
        byte = length % 128
        length //= 128
        encoded.append(byte | (128 if length else 0))
        if not length:
            return bytes([kind]) + encoded + data

async def receive(reader):
    kind = (await reader.readexactly(1))[0]
    length, multiplier = 0, 1
    while True:
        byte = (await reader.readexactly(1))[0]
        length += (byte & 127) * multiplier
        if not byte & 128:
            break
        multiplier *= 128
    return kind, await reader.readexactly(length)

async def device(index, gate, release):
    async with asyncio.timeout(HOLD + 150):
        return await device_session(index, gate, release)

async def device_session(index, gate, release):
    identity = f"bench-{index:04}"
    reader, writer = await asyncio.open_connection(HOST, 1883)
    try:
        connect = string("MQTT") + bytes([4, 194]) + struct.pack("!H", 60)
        connect += string(f"probe-{index}") + string(identity) + string(f"{identity}-sample")
        writer.write(packet(16, connect))
        await writer.drain()
        kind, reply = await receive(reader)
        if kind != 32 or reply != b"\x00\x00":
            raise RuntimeError(f"connection rejected {identity}")
        writer.write(packet(130, b"\x00\x01" + string(f"devices/{identity}/receipts") + b"\x00"))
        await writer.drain()
        kind, reply = await receive(reader)
        if kind != 144 or reply[-1] == 128:
            raise RuntimeError(f"subscription rejected {identity}")
        gate[index] = True
        await release.wait()
        batch = SPECS[index]
        event = batch["events"][0]
        began = time.monotonic()
        for attempt in range(ATTEMPTS):
            if time.monotonic() - began > 120:
                break
            writer.write(packet(50, string(f"devices/{identity}/telemetry") +
                                struct.pack("!H", attempt + 2) + json.dumps(batch).encode()))
            await writer.drain()
            try:
                async with asyncio.timeout(10):
                    while True:
                        kind, payload = await receive(reader)
                        if kind >> 4 == 3:
                            size = struct.unpack("!H", payload[:2])[0]
                            receipt = json.loads(payload[2 + size:])
                            if receipt["batch_id"] == batch["batch_id"]:
                                if receipt["outcome"] == "committed":
                                    return {"device": identity, "event": event,
                                            "confirmation_ms": (time.monotonic()-began)*1000, "retries": attempt}
                                break
            except TimeoutError:
                pass
            delay = min(0.1 * 2**attempt, 3)
            await asyncio.sleep(random.uniform(delay / 2, delay) if JITTER else delay)
        raise RuntimeError(f"receipt exhausted {identity}")
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    print(json.dumps({"phase": "manifest", "expected": [
        {"device": f"bench-{index:04}", "event": spec["events"][0]}
        for index, spec in enumerate(SPECS)
    ]}), file=sys.stderr, flush=True)
    gate = {}
    release = asyncio.Event()
    tasks = [asyncio.create_task(device(i, gate, release)) for i in range(COUNT)]
    began = time.monotonic()
    while len(gate) < COUNT:
        if time.monotonic() - began > 30 or any(t.done() and t.exception() for t in tasks):
            release.set()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            raise RuntimeError([str(r) for r in results if isinstance(r, Exception)])
        await asyncio.sleep(0.05)
    await asyncio.sleep(HOLD)
    print(json.dumps({"phase": "publishing", "clients": COUNT}), file=sys.stderr, flush=True)
    release.set()
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    failures = [str(row) for row in outcomes if isinstance(row, BaseException)]
    rows = [row for row in outcomes if isinstance(row, dict)]
    if failures:
        print(json.dumps({"clients": COUNT, "confirmed": len(rows), "errors": failures,
                          "expected": [{"device": f"bench-{i:04}", "event": spec["events"][0]}
                                       for i, spec in enumerate(SPECS)]}))
        raise SystemExit(1)
    ordered = sorted(row["confirmation_ms"] for row in rows)
    print(json.dumps({"clients": COUNT, "held_seconds": HOLD, "confirmed": len(rows),
                      "p99_ms": ordered[min(len(rows)-1, math.ceil(len(rows)*0.99)-1)],
                      "retries": sum(row["retries"] for row in rows), "attempt_limit": ATTEMPTS, "jitter": JITTER, "expected": rows}))

asyncio.run(main())
