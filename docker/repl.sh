#!/bin/sh

# Script to run in container in Mock mode
set - e

echo "Running REPL."

echo "daemonize yes" | redis-server -

cd /home/deploy/event-data-event-bus
lein repl