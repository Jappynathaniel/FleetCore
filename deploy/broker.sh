#!/bin/sh
set -eu
umask 027
# A non-sticky, root-owned directory permits safe regeneration after Mosquitto
# takes ownership of the files. Hardened Linux hosts reject that write in /tmp.
mkdir -p /run/fleetcore
chmod 0755 /run/fleetcore
mosquitto_passwd -b -c /run/fleetcore/passwords fleetcore-service service-broker-demo
mosquitto_passwd -b /run/fleetcore/passwords north-van north-van-demo
mosquitto_passwd -b /run/fleetcore/passwords south-van south-van-demo
cp /fleetcore/mosquitto.acl /run/fleetcore/acl
chown mosquitto:mosquitto /run/fleetcore/passwords /run/fleetcore/acl
chmod 0600 /run/fleetcore/passwords /run/fleetcore/acl
exec mosquitto -c /fleetcore/mosquitto.conf
