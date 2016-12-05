#!/bin/sh

# Script to run in production.
set - e

echo "Running production Event Bus."

cd /home/deploy/event-data-event-bus

lein run
