# Event Data Event Bus Mock
# Production build of Crossref Event Data Event Bus

FROM ubuntu
MAINTAINER Joe Wass jwass@crossref.org

RUN apt-get update
RUN apt-get -y install openjdk-8-jdk-headless
RUN apt-get -y install curl

RUN groupadd -r deploy && useradd -r -g deploy deploy
RUN mkdir /home/deploy

RUN curl https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein > /usr/bin/lein
RUN chmod a+x /usr/bin/lein

COPY . /home/deploy/event-data-event-bus

RUN chown -R deploy /home/deploy

USER deploy
RUN cd /home/deploy/event-data-event-bus && lein deps && lein compile
