#!/bin/sh

# Script to run in container in Mock mode
set - e

echo "Running INTEGRATION tests."

echo "daemonize yes" | redis-server -

cd /home/deploy/event-data-event-bus

export PORT="9990"
export JWT_SECRETS="TEST,TEST2"
export REDIS_HOST="127.0.0.1"
export REDIS_PORT="6379"
export REDIS_DB="0"
export STORAGE="s3"


lein clean # useful for cached protocols

# These are actually the component test suite, except with the above S3 configuration.

lein test :component
