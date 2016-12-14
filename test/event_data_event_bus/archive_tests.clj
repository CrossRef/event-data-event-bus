(ns event-data-event-bus.archive-tests
  "Tests for the archive namespace.
   NOTE: The round-trip archive test via the API is tested in server-tests."
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [clojure.tools.logging :as l]
            [config.core :refer [env]]
            [event-data-event-bus.archive :as archive]
            [event-data-common.storage.store :as store]
            [event-data-common.storage.redis :as redis]
            [ring.mock.request :as mock]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clojure.java.io :as io])
  (:import [com.auth0.jwt JWTSigner JWTVerifier]))

(def redis-store
  "A redis connection for storing subscription and short-term information."
  (delay (redis/build "test" (:redis-host env) (Integer/parseInt (:redis-port env)) (Integer/parseInt (get env :redis-db)))))

(deftest ^:component should-archive-day
  ; This is similar to the test in the server-tests but uses the functions directly.
  (let [friday (clj-time/date-time 2016 11 25)
        saturday (clj-time/date-time 2016 11 26)
        sunday (clj-time/date-time 2016 11 27)
        monday (clj-time/date-time 2016 11 28)

        ; just north of 3,000 events, a day's worth of Wikipedia.
        friday-events (take 10 (json/read (io/reader (io/file (io/resource "test/wiki-2016-11-25.json")))))
        saturday-events (take 10 (json/read (io/reader (io/file (io/resource "test/wiki-2016-11-26.json")))))
        sunday-events (take 10 (json/read (io/reader (io/file (io/resource "test/wiki-2016-11-27.json")))))]

    (clj-time/do-at friday
      (doseq [event friday-events]
        (archive/save-event @redis-store (event "id") "2016-11-25" (json/write-str event))))

    (clj-time/do-at saturday
      (doseq [event saturday-events]
        (archive/save-event @redis-store (event "id") "2016-11-26" (json/write-str event))))

    (clj-time/do-at sunday
      (doseq [event sunday-events]
        (archive/save-event @redis-store (event "id") "2016-11-27" (json/write-str event))))

    ; (let [saturday-archive (archive/archive-for @redis-store "2016-11-26")
          ; saturday-archive-events (get saturday-archive "events")]
      ; (is (= (count saturday-events) (count saturday-archive-events)) "All events returned")
      ; (is (= (set (map #(% "id") saturday-events))
             ; (set (map #(% "id") saturday-archive-events))) "All events returned")
      ; )

    ))

