#!/bin/sh

# Build and update
set - e

docker build -f Dockerfile.prod -t crossref/event-data-event-bus .

docker push crossref/event-data-event-bus
