#!/bin/sh

# Script to run in container in Mock mode
set - e

echo "Running INTEGRATION tests."

echo "daemonize yes" | redis-server -

cd /home/deploy/event-data-event-bus
lein test :integration