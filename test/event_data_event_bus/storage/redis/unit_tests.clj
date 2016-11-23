(ns event-data-event-bus.storage.redis.unit-tests
  "Unit tests for Redis."
  (:require [clojure.test :refer :all]
            [event-data-event-bus.storage.redis :as redis]))

(deftest ^:unit add-remove-prefix
  (testing "Prefix can be added and removed"
    (let [original "one two three"
          prefixed (redis/add-prefix original)
          unprefixed (redis/remove-prefix prefixed)]
      (is (not= original prefixed) "Prefix is added")
      (is (not= prefixed unprefixed) "Prefix is removed")
      (is (= original unprefixed) "Correct prefix is removed"))))

