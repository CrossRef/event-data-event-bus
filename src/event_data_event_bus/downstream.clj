(ns event-data-event-bus.downstream
  "Send Events on their way downstream.
  Configuration should be supplied in a JSON file specified at the path given by BUS_BROADCAST_CONFIG.
  Example of config:

  {
    'http-post-live': {
      'example.com post': {
        'jwt': 'abcdefgh',
        'endpoint': 'http://example.com/events'
      }
    },
    'kafka-live': {
      'my kafka': {
        'bootstrap-servers': 'localhost:9092,other:9092',
        'topic': 'my-topic'
      }
    }
  }"

  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clojure.string :as string]
            [config.core :refer [env]]
            [event-data-common.backoff :as backoff]
            [org.httpkit.client :as client]
            [clojure.set :refer [superset?]])
  (:import [org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecords]
           [org.apache.kafka.common TopicPartition PartitionInfo]))

(def retries
  "Try to deliver this many times downstream."
  5)

(def retry-delay
  "Wait in milliseconds before first redelivery attempt. Retry will double each time."
  (* 10 1000))

(defn load-broadcast-config
  []
  "Load broadcast configuration structure from environment variable config."
   (let [config-path (:bus-broadcast-config env)
         value (json/read-str (slurp config-path) :key-fn keyword)]
      (log/debug "Loaded broadcast config from " config-path)
   value))

; Map of config label to Kafka producer.
(def kafka-producers (atom {}))

(defn get-kafka-producer
  "Get a Kafka Producer by its label, with config supplied."
  [config label]

  (if-let [producer (@kafka-producers label)]
    producer
    (let [properties (java.util.Properties.)
          bootstrap-servers (:bootstrap-servers config)]
      
      (when (clojure.string/blank? bootstrap-servers)
        (throw (IllegalArgumentException. (str "bootstrap-servers was blank for config label " label))))

      (.put properties "bootstrap.servers" bootstrap-servers)
      (.put properties "acks", "all")
      (.put properties "retries", (int 5))
      (.put properties "key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      (.put properties "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      (let [producer (KafkaProducer. properties)]
        (swap! kafka-producers #(assoc % label producer))
        producer))))

(defn send-kafka-producer
  [producer topic event-id event-json]
  (.send producer
         (ProducerRecord. topic
                          event-id
                          event-json)))

(def downstream-config-cache (delay (load-broadcast-config)))

(defn broadcast-live
  "Accept incoming event and broadcast to live downstream subscribers."
  [event-structure]
  ; Most of the heavy lifting is done by `backoff`.
  (let [id (:id event-structure)
        body-json (json/write-str event-structure)]
    (log/debug "Recieve event for live broadcast:" id "to" (count @downstream-config-cache))
    
    ; Send to Kafka.
    (doseq [[label config] (:kafka-live @downstream-config-cache)]
      (let [producer (get-kafka-producer config label)]
        (when-not producer
          (throw (IllegalArgumentException. (str "Can't create Kafka Producer for label " label))))

        (send-kafka-producer producer (:topic config) id body-json)))

    ; Send to HTTP posts.
    (doseq [[label config] (:http-post-live @downstream-config-cache)]
      (backoff/try-backoff
        ; Exception thrown if not 200 or 201, also if some other exception is thrown during the client posting.
        #(let [response @(client/post (:endpoint config)
                                      {:headers {"authorization" (str "Bearer " (:jwt config))}
                                       :body body-json})]
            (when-not (#{201 200} (:status response))
              (throw (new Exception (str "Failed to send to downstream " label "at URL: " (:endpoint config) " with status code: " (:status response))))))
        retry-delay
        retries
        ; Only log info on retry because it'll be tried again.
        #(log/info "Error broadcasting" id "to downstream" label "with exception" (.getMessage %))
        ; But if terminate is called, that's a serious problem.
        #(log/error "Failed to send event" id "to downstream" label)
        #(log/debug "Finished broadcasting" id "to downstream" label)))))
