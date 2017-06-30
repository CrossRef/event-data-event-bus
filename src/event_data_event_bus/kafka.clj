(ns event-data-event-bus.kafka
  "Monitor a Kafka topic and ingest Events from it.
  Use the same interface as the server's POST interface."
  (:require 
    [config.core :refer [env]]
    [event-data-event-bus.server :as server]
    [clojure.data.json :as json]
    [event-data-common.jwt :as jwt]
    [clojure.tools.logging :as log])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecords]))

(def verifier (jwt/build (:global-jwt-secrets env)))

(defn input-event->ring-request
  "Transform an incoming JSON representation of an Event into a Ring object that can supplied in a call 
   against the server/events resource.
   Input should contain a JWT, which we have to remove before passing on."
  [event-json]
  (let [event (json/read-str event-json :key-fn keyword)
        the-jwt (:jwt event)
        input-event (dissoc event :jwt)
        body (java.io.StringReader. (json/write-str input-event))]
    {:body body
     :request-method :post
      :jwt-claims (jwt/try-verify verifier the-jwt)}))

(defn run-ingest-kafka
  []
  (let [properties (java.util.Properties.)]
     (.put properties "bootstrap.servers" (:global-kafka-bootstrap-servers env))
     (.put properties "group.id"  "bus-input")
     (.put properties "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     (.put properties "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     
     ; This is only used in the absence of an existing marker for the group.
     (.put properties "auto.offset.reset" "earliest")

     (let [consumer (KafkaConsumer. properties)
           topic-name (:global-event-input-topic env)
           server-f (server/events)]
       (log/info "Subscribing to" topic-name)
       (.subscribe consumer (list topic-name))
       (log/info "Subscribed to" topic-name "got" (count (or (.assignment consumer) [])) "assigned partitions")
       (loop []
         (log/info "Polling...")
         (let [^ConsumerRecords records (.poll consumer (int 10000))]
           
           (log/info "Got" (.count records) "records." (.hashCode records))
           (doseq [^ConsumerRecords record records]
             (try
              (let [result (server-f (input-event->ring-request (.value record)))]
                
                ; Created or already exists (for duplicates).
                (when-not (#{201 409} (:status result))
                  (log/error "Failed to process Event ID" (.key record) "got result" result)))

              (catch Exception ex (do
                (.printStackTrace ex)
                (log/error "Exception processing Event ID" (.key record) (.getMessage ex)))))))

          (recur)))))
