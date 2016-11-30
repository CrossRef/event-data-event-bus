#!/bin/sh

# Script to run in container in Mock mode
set - e

: ${S3_KEY?"Need to set S3_KEY"}
: ${S3_SECRET?"Need to set S3_SECRET"}
: ${S3_BUCKET_NAME?"Need to set S3_BUCKET_NAME"}
: ${S3_REGION_NAME?"Need to set S3_REGION_NAME"}

docker run -e "S3_REGION_NAME=$S3_REGION_NAME" -e "S3_KEY=$S3_KEY" -e "S3_SECRET=$S3_SECRET" -e "S3_BUCKET_NAME=$S3_BUCKET_NAME" --entrypoint=/home/deploy/event-data-event-bus/docker/integration-tests.sh -p 9990:9990 -v `pwd`:/home/deploy/event-data-event-bus -a stdout -it crossref/event-data-event-bus-mock
