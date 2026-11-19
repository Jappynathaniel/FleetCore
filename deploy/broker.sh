#!/bin/sh
set -eu
umask 027
mosquitto_passwd -b -c /tmp/fleetcore-passwords fleetcore-service service-broker-demo
mosquitto_passwd -b /tmp/fleetcore-passwords north-van north-van-demo
mosquitto_passwd -b /tmp/fleetcore-passwords south-van south-van-demo
cp /fleetcore/mosquitto.acl /tmp/fleetcore-acl
chown mosquitto:mosquitto /tmp/fleetcore-passwords /tmp/fleetcore-acl
chmod 0600 /tmp/fleetcore-passwords /tmp/fleetcore-acl
exec mosquitto -c /fleetcore/mosquitto.conf
