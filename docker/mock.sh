#!/bin/sh

# Script to run in container in Mock mode
set - e

echo "Running mock instance."

echo "daemonize yes" | redis-server -

cd /home/deploy/event-data-event-bus

export MOCK="TRUE"
export PORT="9990"
export JWT_SECRETS="TEST,TEST2"
export REDIS_HOST="127.0.0.1"
export REDIS_PORT="6379"
export REDIS_DB="0"
export STORAGE="redis"

lein clean # useful for cached protocols
lein run
