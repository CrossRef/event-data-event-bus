# Event Data Event Bus Mock
# Production build of Crossref Event Data Event Bus

FROM lein-2.7.1-alpine
MAINTAINER Joe Wass jwass@crossref.org

COPY . /usr/src/app
WORKDIR /usr/src/app

RUN lein deps && lein compile
