(ns event-data-event-bus.storage.redis
  "Storage interface for redis. Stores all keys with a prefix."
  (:require [config.core :refer [env]])
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

; Below are factory functions.

(defn get-value
  [pool]
  "Factory function to return 'get value for key' function."
  (fn [k]
    (with-open [conn (get-connection pool)]
      (.get conn (str prefix k)))))

(defn set-value
  [pool]
  "Factory function to return 'set value for key' function."
  (fn [k v]
    (with-open [conn (get-connection pool)]
      (.set conn (str prefix k) v))))

(defn build
  "Build an interface dict"
  []
  (let [pool (make-jedis-pool)]
    {:get (get-value pool)
     :set (set-value pool)}))
