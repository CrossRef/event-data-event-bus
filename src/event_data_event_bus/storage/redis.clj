(ns event-data-event-bus.storage.redis
  "Storage interface for redis. Satisfies the event-data-event-bus.storage.storage.Store protocol.
   Also provides other Redis-specific methods.
   All keys are stored in Redis with the given prefix."
  (:require [config.core :refer [env]]
            [event-data-event-bus.storage.store :refer [Store]])
  (:import [redis.clients.jedis Jedis JedisPool JedisPoolConfig]))

(def prefix
  "Unique prefix applied to every key."
  "event-data-bus:")

(def default-db-str "0")

(defn remove-prefix
  [k]
  (when k(.substring k 15)))

(defn add-prefix
  [k]
  (str prefix k))

(defn make-jedis-pool
  []
  (let [pool-config (new org.apache.commons.pool2.impl.GenericObjectPoolConfig)]
    (.setMaxTotal pool-config 100)
  (new JedisPool pool-config (:redis-host env) (Integer/parseInt (:redis-port env)))))

(defn get-connection
  "Get a Redis connection from the pool. Must be closed."
  [pool]
  (let [resource (.getResource pool)]
    (.select resource (Integer/parseInt (get env :redis-db default-db-str)))
    resource))

(defn get-string
  [pool k]
  (with-open [conn (get-connection pool)]
    (.get conn (str prefix k))))

(defn set-string
  [pool k v]
  (with-open [conn (get-connection pool)]
    (.set conn (str prefix k) v)))

(defrecord Redis
  [pool]
  Store
  (get-string [this k] (get-string pool k))
  (set-string [this k v] (set-string pool k v)))

(defn build
  "Build a Store object."
  []
  (let [pool (make-jedis-pool)]
    (Redis. pool)))
