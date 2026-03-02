#!/usr/bin/env bash
set -e

make build

params=(--host="$HOST" --username="$USERNAME" --password="$PASSWORD" --database="$DATABASE" --table="$TABLE" --alter="engine=innodb")

if [ -n "$REPLICA_DSN" ]; then
  params+=(--replica-dsn="$REPLICA_DSN")
fi

./bin/spirit migrate "${params[@]}"
