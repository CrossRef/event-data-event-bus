docker run --entrypoint=/home/deploy/event-data-event-bus/docker/unit-tests.sh -p 9990:9990 -v `pwd`:/home/deploy/event-data-event-bus -a stdout -it crossref/event-data-event-bus-mock
