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


(deftest ^:component setex-and-get
  (testing "Key can be set with expiry and retrieved."
    (let [ki "this is my immediately expiring key"
          kl "this is my long expiring key"
          vi "this is my immediately expiring value"
          vl "this is my long expiring value"

          ; build a redis connection with the present configuration.
          redis-conn (redis/build)]
      ; Set immediately expiring key and one that expires after 100 seconds.
      (redis/set-string-and-expiry redis-conn ki 1 vi)
      (redis/set-string-and-expiry redis-conn kl 100000 vl)

      ; A brief nap should be OK.
      (Thread/sleep 2)

      (is (= nil (store/get-string redis-conn ki)) "Expired value should not be returned")
      (is (= vl (store/get-string redis-conn kl)) "Long expiring value should be returned"))))

(deftest ^:component expiring-mutex
  (testing "Expiring mutex can only be set once in expiry time.")
  (let [redis-conn (redis/build)
        k "my key"]
    ; First set should be OK.
    (is (true? (redis/expiring-mutex!? redis-conn k 1000)) "First set to mutex for key should be true.")

    ; Second should be false. Also reset timing of mutex.
    (is (false? (redis/expiring-mutex!? redis-conn k 1)) "Second set to mutex for key should be false.")

    ; Let it expire for a couple of milliseconds.
    (Thread/sleep 2)

    (is (true? (redis/expiring-mutex!? redis-conn k 1000)) "Access to mutex should be true after expiry.")))

