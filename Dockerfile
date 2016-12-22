# Event Data Event Bus Mock
# Production build of Crossref Event Data Event Bus

FROM clojure:lein-2.7.0-alpine
MAINTAINER Joe Wass jwass@crossref.org

COPY . /usr/src/app
WORKDIR /usr/src/app

RUN lein deps && lein compile
