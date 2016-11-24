#!/bin/sh

# Script to run in container in Mock mode
set - e

echo "Running ALL tests"

echo "daemonize yes" | redis-server -

cd /home/deploy/event-data-event-bus
lein clean # useful for cached protocols
lein test :all