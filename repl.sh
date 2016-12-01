# Include S3 config. It may or may not be present.

docker run -e "S3_REGION_NAME=$S3_REGION_NAME" -e "S3_KEY=$S3_KEY" -e "S3_SECRET=$S3_SECRET" -e "S3_BUCKET_NAME=$S3_BUCKET_NAME" --entrypoint=/home/deploy/event-data-event-bus/docker/repl.sh -p 9990:9990  -a stdout -v `pwd`:/home/deploy/event-data-event-bus -it crossref/event-data-event-bus-mock