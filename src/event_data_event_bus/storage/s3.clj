(ns event-data-event-bus.storage.s3
  "Storage interface for AWS S3. Currently faked out."
  (:require [config.core :refer [env]]
            [event-data-event-bus.storage.store :refer [Store]])
  (:import [redis.clients.jedis Jedis JedisPool JedisPoolConfig]))

(defrecord S3Connection
  []
  
  Store
  (get-string [this k]
    (throw UnsupportedOperationException))

  (set-string [this k v]
    (throw UnsupportedOperationException)))

(defn build
  "Build a S3Connection object."
  []
  (S3Connection.))

