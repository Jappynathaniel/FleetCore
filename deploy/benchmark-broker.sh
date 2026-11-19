#!/bin/sh
set -eu
umask 027
mosquitto_passwd -b -c /tmp/passwords fleetcore-service service-broker-demo
i=0
while [ "$i" -lt 1000 ]; do
    id=$(printf 'bench-%04d' "$i")
    mosquitto_passwd -b /tmp/passwords "$id" "$id-sample"
    i=$((i+1))
done
cat > /tmp/acl <<'ACL'
user fleetcore-service
topic read devices/+/telemetry
topic write devices/+/receipts
pattern write devices/%u/telemetry
pattern read devices/%u/receipts
ACL
chown mosquitto:mosquitto /tmp/passwords /tmp/acl
chmod 0600 /tmp/passwords /tmp/acl
printf '%s\n' 'listener 1883' 'allow_anonymous false' 'password_file /tmp/passwords' 'acl_file /tmp/acl' > /tmp/broker.conf
exec mosquitto -c /tmp/broker.conf
