#!/bin/sh
set -eu
umask 027
mosquitto_passwd -b -c /tmp/fleetcore-passwords fleetcore-service service-broker-demo
mosquitto_passwd -b /tmp/fleetcore-passwords north-van north-van-demo
mosquitto_passwd -b /tmp/fleetcore-passwords south-van south-van-demo
chown mosquitto:mosquitto /tmp/fleetcore-passwords
exec mosquitto -c /fleetcore/mosquitto.conf
