(ns event-data-event-bus.storage.redis
  "Storage interface for redis. Provides two interfaces: RedisStore which conforms to Store, and Redis, which contains Redis-specific methods.
   RedisStore satisfies the event-data-event-bus.storage.storage.Store protocol.
   All keys are stored in Redis with the given prefix."
  (:require [config.core :refer [env]]
            [event-data-event-bus.storage.store :refer [Store]])
  (:import [redis.clients.jedis Jedis JedisPool JedisPoolConfig]))

(def prefix
  "Unique prefix applied to every key."
  "event-data-bus:")

(def default-db-str "0")

(defn remove-prefix
  [^String k]
  (when k (.substring k 15)))

(defn add-prefix
  [^String k]
  (str prefix k))

(defn make-jedis-pool
  []
  (let [pool-config (new org.apache.commons.pool2.impl.GenericObjectPoolConfig)]
    (.setMaxTotal pool-config 100)
  (new JedisPool pool-config (:redis-host env) (Integer/parseInt (:redis-port env)))))

(defn ^Jedis get-connection
  "Get a Redis connection from the pool. Must be closed."
  [^JedisPool pool]
  (let [^Jedis resource (.getResource pool)]
    (.select resource (Integer/parseInt (get env :redis-db default-db-str)))
    resource))

(defprotocol Redis
  "Redis-specific interface."
  (set-string-and-expiry [this k v milliseconds] "Set string value with expiry in milliseconds.")

  (expiring-mutex!? [this k milliseconds] "Check and set expiring mutex atomically, returning true if didn't exist."))

(defrecord RedisConnection
  [^JedisPool pool]
  
  Store
  (get-string [this k]
    (with-open [conn (get-connection pool)]
      (.get conn (add-prefix k))))

  (set-string [this k v]
    (with-open [ conn (get-connection pool)]
    (.set conn (add-prefix k) v)))

  Redis
  (set-string-and-expiry [this k milliseconds v]
    (with-open [conn (get-connection pool)]
      (.psetex conn (add-prefix k) milliseconds v)))

  (expiring-mutex!? [this k milliseconds]
    (with-open [conn (get-connection pool)]
      ; Set a token value. SETNX returns true if it wasn't set before.
      (let [success (= 1 (.setnx conn (add-prefix k) "."))]
        (.pexpire conn (add-prefix k) milliseconds)
        success))))

(defn build
  "Build a RedisConnection object."
  []
  (let [pool (make-jedis-pool)]
    (RedisConnection. pool)))
