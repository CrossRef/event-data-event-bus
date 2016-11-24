(ns event-data-event-bus.storage.redis.component-tests
  "Component tests for Redis."
  (:require [clojure.test :refer :all]
            [event-data-event-bus.storage.redis :as redis]
            [event-data-event-bus.storage.store :as store]))

(deftest ^:component set-and-get
  (testing "Key can be set and retrieved."
    (let [k "this is my key"
          v "this is my value"
          ; build a redis connection with the present configuration.
          redis (redis/build)]
      (store/set-string redis k v)
      (is (= v (store/get-string redis k)) "Correct value returned"))))

