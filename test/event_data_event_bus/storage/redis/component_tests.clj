(ns event-data-event-bus.storage.redis.component-tests
  "Component tests for Redis."
  (:require [clojure.test :refer :all]
            [event-data-event-bus.storage.redis :as redis]))

(deftest ^:unit add-remove-prefix
  (testing "Key can be set and retrieved."
    (let [k "this is my key"
          v "this is my value"
          ; build a redis connection with the present configuration.
          redis (redis/build)]
      ((:set redis) k v)
      (is (= v ((:get redis) k)) "Correct value returned"))))
