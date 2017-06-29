(ns event-data-event-bus.server-tests
  "Tests for the server namespace.
   Most component tests work at the Ring level, including routing.
   These run through all the middleware including JWT extraction."
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [clojure.tools.logging :as l]
            [event-data-event-bus.server :as server]
            [event-data-event-bus.archive :as archive]
            [event-data-common.storage.store :as store]
            [event-data-common.date :as date]
            [ring.mock.request :as mock]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clojure.java.io :as io]
            [org.httpkit.fake :as fake]
            [config.core :refer [env]])
  (:import [com.auth0.jwt JWTSigner JWTVerifier]))

(def signer
  "A JWT signer that uses a secret we expect."
  (delay (new JWTSigner "TEST")))

(def unrecognised-signer
  "A JWT signer that uses a secret we don't expect."
  (delay (new JWTSigner "UNRECOGNISED")))

(def source-id "1111111111")

(def matching-token
  "A token that is correct for the event being sent (i.e. correct `sub`)."
  (delay (.sign @signer {"sub" source-id})))

(def not-matching-token
  "A token that is correctly signed but unsuitable for the event being sent (i.e. incorrect `sub`)."
  (delay (.sign @signer {"sub" "2222222222"})))

(def unrecognised-token
  "A token that used the wrong signer but claims to be the right source."
  (delay (.sign @unrecognised-signer {"sub" "1111111111"})))

(def invalid-token
  "A token from another universe."
  (delay "PLEASE_LET_ME_IN"))

; This fixture is run for every test, including unit tests (though it's not required for unit tests).
(defn clear-storage-fixture
  [f]
  ; Clear all keys from storage (could be Redis or S3 depending on config)
  (doseq [k (store/keys-matching-prefix @server/storage "")]
    (store/delete @server/storage k))

  ; Clear all keys from Redis when we're using it (i.e. not unit tests).
  (when (:bus-redis-host env)
    (doseq [k (store/keys-matching-prefix @server/redis-store "")]
      (store/delete @server/redis-store k)))
 
  ; For these tests we want to eat the downstream broadcasts and status updates.
  (fake/with-fake-http ["http://endpoint1.com/endpoint" {:status 201}
                        "http://endpoint2.com/endpoint" {:status 201}]
    (f)))

(use-fixtures :each clear-storage-fixture)

; Ingestion, validation, authorization

(deftest ^:unit timestamp-event
  (testing "Events can be timestamped"
    (let [original-event {:contents :do :not :matter}
          [timestamped-event yyyy-mm-dd] (server/timestamp-event original-event)]

      (is (not (:timestamp original-event)) "Pre: timestamp not in original event.")

      (is (clojure.set/subset? (set original-event) (set timestamped-event)) "Original keys and values should be present.")
      (is (:timestamp timestamped-event) "Timestamp added to event")
      (is yyyy-mm-dd "YYYY-MM-DD returned")
      (is (.startsWith (:timestamp timestamped-event) yyyy-mm-dd) "YYYY-MM-DD is consistent with timestamp"))))

(deftest ^:component should-accept-good-events
  (testing "Submitted events with correctly signed JWT that matches the `subj` of the `source_id` should be accepted."
    ; Don't broadcast.
    (with-redefs [event-data-event-bus.server/broadcast-live-async (fn [_])]
      (let [event  {"obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
                    "occurred_at" "2016-09-25T23:58:58Z"
                    "subj_id" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                    "total" 1
                    "id" "EVENT-ID-1"
                    "subj" {
                      "pid" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "title" "Señalización paracrina"
                      "issued" "2016-09-25T23:58:58.000Z"
                      "URL" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "type" "entry-encyclopedia"}
                    "message_action" "create"
                    "source_id" source-id
                    "relation_type_id" "references"}
          
            token (.sign @signer {"sub" source-id})
            request (->
                (mock/request :post "/events")
                (mock/header "authorization" (str "Bearer " @matching-token))
                (mock/body (json/write-str event)))
            response (@server/app request)

            ; After we sent it, try and look it up on the API.
            get-event-response (->
                                 (mock/request :get "/events/EVENT-ID-1")
                                 (mock/header "authorization" (str "Bearer " @matching-token))
                                 (@server/app))]

        (is (= 201 (:status response)) "Should be created OK.")
        ; The returned event will have added field(s) e.g. timestamp, so won't be identical. Should be a proper superset though.

        (is (clojure.set/subset? (set event)
                                 (set (json/read-str (:body get-event-response))))) "Same event should be retrievable afterwards."))))

(deftest ^:component should-reject-events-not-matching-token
  (testing "Submitted events with correctly signed JWT but non-matching `subj` of the `source_id` should be rejected."
    (let [event (json/write-str
                  {"obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
                    "occurred_at" "2016-09-25T23:58:58Z"
                    "subj_id" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                    "total" 1
                    "id" "EVENT-ID-2"
                    "subj" {
                      "pid" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "title" "Señalización paracrina"
                      "issued" "2016-09-25T23:58:58.000Z"
                      "URL" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "type" "entry-encyclopedia"}
                    "message_action" "create"
                    "source_id" source-id
                    "relation_type_id" "references"})
          token (.sign @signer {"sub" "my-service"})
          request (->
              (mock/request :post "/events")
              (mock/header "authorization" (str "Bearer " @not-matching-token))
              (mock/body event))
          response (@server/app request)]
      (is (= 403 (:status response)) "Should be forbidden"))))

(deftest ^:component should-reject-unsigned-events
  (testing "Submitted events with incorrect JWT should be accepted."
    (let [event (json/write-str
                  {"obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
                    "occurred_at" "2016-09-25T23:58:58Z"
                    "subj_id" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                    "total" 1
                    "id" "EVENT-ID-3"
                    "subj" {
                      "pid" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "title" "Señalización paracrina"
                      "issued" "2016-09-25T23:58:58.000Z"
                      "URL" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "type" "entry-encyclopedia"}
                    "message_action" "create"
                    "source_id" source-id
                    "relation_type_id" "references"})
          token (.sign @signer {"sub" "my-service"})
          request (->
              (mock/request :post "/events")
              (mock/header "authorization" (str "Bearer " @not-matching-token))
              (mock/body event))
          response (@server/app request)]
      (is (= 403 (:status response)) "Should be forbidden"))))

(deftest ^:component should-reject-events-not-passing-schema
  (testing "Submitted events should be subject to schema."
    (let [event (json/write-str
                  {"obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
                    "occurred_at" "2016-09-25T23:58:58Z"
                    ; subj_id missing
                    "total" 1
                    "id" "EVENT-ID-5"
                    "subj" {
                      "pid" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "title" "Señalización paracrina"
                      "issued" "2016-09-25T23:58:58.000Z"
                      "URL" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "type" "entry-encyclopedia"}
                    "message_action" "create"
                    "source_id" source-id
                    "relation_type_id" "references"})
          token (.sign @signer {"sub" "my-service"})
          request (->
              (mock/request :post "/events")
              (mock/header "authorization" (str "Bearer " @matching-token))
              (mock/body event))
          response (@server/app request)]
      (is (= 400 (:status response)) "Should be malformed")
      (is (.contains (-> :body response) "schema-errors") "Should say something about schema errors"))))


(deftest ^:component should-reject-duplicate-events
  (with-redefs [event-data-event-bus.server/broadcast-live-async (fn [_])]
    (let [event (json/write-str
                  {"obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
                    "occurred_at" "2016-09-25T23:58:58Z"
                    "subj_id" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                    "total" 1
                    "id" "EVENT-ID-4"
                    "subj" {
                      "pid" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "title" "Señalización paracrina"
                      "issued" "2016-09-25T23:58:58.000Z"
                      "URL" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                      "type" "entry-encyclopedia"}
                    "message_action" "create"
                    "source_id" source-id
                    "relation_type_id" "references"})
          token (.sign @signer {"sub" source-id})

          ; Send the same thing twice.
          first-response (@server/app (-> (mock/request :post "/events")
                                         (mock/header "authorization" (str "Bearer " @matching-token))
                                         (mock/body event)))

          second-response (@server/app (-> (mock/request :post "/events")
                                         (mock/header "authorization" (str "Bearer " @matching-token))
                                         (mock/body event)))]

        (is (= 201 (:status first-response)) "Should be created OK.")
        (is (= 409 (:status second-response)) "Duplicate should be rejected."))))

(deftest ^:component should-assign-timestamps
  ; TODO
)

(deftest ^:component live-archive-should-reject-bad-dates
  ; Pick a day to be 'today'.
  (let [today-midnight (clj-time/date-time 2016 2 5)
        tomorrow-midnight (clj-time/plus today-midnight (clj-time/days 1))
        yesterday-midnight (clj-time/minus today-midnight (clj-time/days 1))]

    (testing "Live Archive should reject badly formatted dates"
      (is (= 400 (:status (@server/app (-> (mock/request :get "/events/archive/fifth-of-september-twenty-sixteen/xx")
                                (mock/header "authorization" (str "Bearer " @matching-token))))))
          "Badly formatted date should be rejected as malformed."))

    (testing "Live Archive should reject dates after today"
      (clj-time/do-at today-midnight
        (let [tomorrow-str (clj-time-format/unparse date/yyyy-mm-dd-format tomorrow-midnight)]
          (is (= 403 (:status (@server/app (-> (mock/request :get (str "/events/archive/" tomorrow-str "/aa"))
                                    (mock/header "authorization" (str "Bearer " @matching-token))))))
              "Tomorrow's date should be forbidden."))))

    (testing "Live Archive should reject requests for dates that end precisely at midnight this morning unless we're at least an hour later."
      (let [yesterday-str (clj-time-format/unparse date/yyyy-mm-dd-format yesterday-midnight)
            ; two hours after midnight this morning
            two-hours-later (clj-time/plus today-midnight (clj-time/hours 2))]
        
        (clj-time/do-at today-midnight
          (is (= 403 (:status (@server/app (-> (mock/request :get (str "/events/archive/" yesterday-str "/aa"))
                                               (mock/header "authorization" (str "Bearer " @matching-token))))))
              "Yesterday's date should be forbidden for the first hour of the day."))

        (clj-time/do-at two-hours-later
          (is (= 200 (:status (@server/app (-> (mock/request :get (str "/events/archive/" yesterday-str "/aa"))
                                               (mock/header "authorization" (str "Bearer " @matching-token))))))
              "Yesterday's date should be OK after the first hour of the day."))))))

; Generation and consumption of live archive and archive.

(deftest ^:component should-archive-events
  ; Don't broadcast.
  (with-redefs [event-data-event-bus.server/broadcast-live-async (fn [_])]

    (let [friday (clj-time/date-time 2016 11 25)
          saturday (clj-time/date-time 2016 11 26)
          sunday (clj-time/date-time 2016 11 27)
          monday (clj-time/date-time 2016 11 28)

          token (.sign @signer {"sub" "wikipedia"})

          ; just north of 3,000 events, a day's worth of Wikipedia.
          friday-events (json/read (io/reader (io/file (io/resource "test/wiki-2016-11-25.json"))))
          saturday-events (json/read (io/reader (io/file (io/resource "test/wiki-2016-11-26.json"))))
          sunday-events (json/read (io/reader (io/file (io/resource "test/wiki-2016-11-27.json"))))]

      (testing "Pre-verification, event IDs from different days are all different."
        (let [friday-ids (set (map #(get %"id") friday-events))
              saturday-ids (set (map #(get %"id") saturday-events))
              sunday-ids (set (map #(get %"id") sunday-events))]

          (is (empty? (clojure.set/intersection friday-ids saturday-ids)))
          (is (empty? (clojure.set/intersection saturday-ids sunday-ids)))
          (is (empty? (clojure.set/intersection friday-ids sunday-ids)))))

      ; A busy weekend of events.
    (l/info "Insert Friday")
    (clj-time/do-at friday
      (doseq [event friday-events]
        (@server/app (->
          (mock/request :post "/events")
          (mock/header "authorization" (str "Bearer " token))
          (mock/body (json/write-str event))))))

    (l/info "Insert Saturday")
    (clj-time/do-at saturday
      (doseq [event saturday-events]
        (@server/app (->
          (mock/request :post "/events")
          (mock/header "authorization" (str "Bearer " token))
          (mock/body (json/write-str event))))))

    (l/info "Insert Sunday")
    (clj-time/do-at sunday
      (doseq [event sunday-events]
        (@server/app (->
          (mock/request :post "/events")
          (mock/header "authorization" (str "Bearer " token))
          (mock/body (json/write-str event))))))

    (testing "Events for specific day can be retrieved as live archive."
      (clj-time/do-at monday
        (let [retrieved-archive-response (@server/app (-> (mock/request :get (str "/events/archive/2016-11-26/aa"))
                                                 (mock/header "authorization" (str "Bearer " @matching-token))))
              {retrieved-events "events" archive-date "archive-generated"} (json/read-str (:body retrieved-archive-response))]

          (is (.startsWith archive-date "2016-11-28") "Archive date should show the time the archive was created.")

          (is (= (count saturday-events)
                 (count retrieved-events)) "Correct number of events should have been returned for the day.")

          (is (every? #(.startsWith (get % "timestamp") "2016-11-26") retrieved-events)
            "All events in a day's archive should have the timestamp of that day.")

          (is (not-any? #(get % "timestamp") saturday-events) "Input events had no timestamps.")
          (is (every? #(get % "timestamp") retrieved-events) "All events should have been given IDs.")

          (is (= (set (map #(get %"id") saturday-events))
                 (set (map #(get %"id") retrieved-events))) "All event IDs sent on Saturday should be returned."))))

    (testing "Empty archive returned when no data."
      (clj-time/do-at monday
        (let [retrieved-archive (@server/app (-> (mock/request :get (str "/events/archive/1901-01-10/aa"))
                                                 (mock/header "authorization" (str "Bearer " @matching-token))))
              {retrieved-events "events"} (json/read-str (:body retrieved-archive))]
          (is (empty? retrieved-events) "On a day when nothing happened the archive should be empty.")))))))

(deftest ^:component archive-should-return-events
  (testing "Archive should contain archived events."
    ; Don't broadcast.
    (with-redefs [event-data-event-bus.server/broadcast-live-async (fn [_])]

      ; Insert some events. 
      ; NB all events in the input file have the same prefix for ease of testing.
      (let [token (.sign @signer {"sub" "wikipedia"})
            friday-events (json/read (io/reader (io/file (io/resource "test/wiki-2016-11-25.json"))))]
          

        (clj-time/do-at (clj-time/date-time 2016 11 25)
          (doseq [event friday-events]
            (@server/app (->
              (mock/request :post "/events")
              (mock/header "authorization" (str "Bearer " token))
                                  (mock/body (json/write-str event))))))


        ; Next day (at 5am) we retrieve from the archive.
        (clj-time/do-at (clj-time/date-time 2016 11 28 5)
          (let [response (@server/app (-> (mock/request :get (str "/events/archive/2016-11-25/aa"))
                                          (mock/header "authorization" (str "Bearer " @matching-token))))

                {events "events" archive-date "archive-generated"} (json/read-str (:body response))]

              ; Now they should be available.
              (is (= 200 (:status response))
                "When the archive has been triggered, there should be events.")

              (is (.startsWith archive-date "2016-11-28") "Archive date should be the date that the archive was generated")

              (is (not-empty events) "Response should contain events.")

              (is (= (count events) (count friday-events)) "Right number of events should be returned.")))))))

(deftest ^:component update-events-roundtrip
  (testing "Should be able to push, retrieve in archive, update, then get new archive."

    ; Don't broadcast.
    (with-redefs [event-data-event-bus.server/broadcast-live-async (fn [_])]

      ; Insert some events. 
      ; NB all events in the input file have the same prefix for ease of testing.
      (let [token (.sign @signer {"sub" "wikipedia"})
            ; An Event we send.
            original-event {
              :total 1
              :source_token "36c35e23-8757-4a9d-aacf-345e9b7eb50d"
              :obj {},
              :id "aad96887-7ab9-43b7-a425-c9058c9e8e20"
              :message_action "create"
              :message_type "relation"
              :subj_id "https://species.wikimedia.org/wiki/Pristimantis_colomai"
              :state "done"
              :subj {
                :pid "https://species.wikimedia.org/wiki/Pristimantis_colomai"
                :title "Pristimantis colomai"
                :issued "2016-11-25T23:41:09.000Z"
                :URL "https://species.wikimedia.org/wiki/Pristimantis_colomai"
                :type "entry-encyclopedia"}
              :source_id "wikipedia"
              :relation_type_id "references"
              :obj_id "https://doi.org/10.11646/zootaxa.4193.3.9"
              :occurred_at "2016-11-25T23:41:09Z"}

           ; And the update we're going to send after.
           updated-event (assoc
                          original-event
                          :updated "deleted"
                          :updated_reason "http://example.com/alternative-facts"
                          :updated_date "2017-01-01")]

        (clj-time/do-at (clj-time/date-time 2016 11 25)
          (@server/app (->
            (mock/request :post "/events")
            (mock/header "authorization" (str "Bearer " token))
            (mock/body (json/write-str original-event)))))


        ; Next day (at 5am) we retrieve from the archive.
        (clj-time/do-at (clj-time/date-time 2016 11 28 5)
          (let [response (@server/app (-> (mock/request :get (str "/events/archive/2016-11-25/aa"))
                                          (mock/header "authorization" (str "Bearer " @matching-token))))

                event (-> response :body (json/read-str :key-fn keyword) :events first)]

            (is (= original-event (dissoc event :timestamp)) "Event should be retrieved from archive (less timestamp which is added)")))

        (clj-time/do-at (clj-time/date-time 2016 11 30)
          ; Then we update that Event.
          (let [update-response (@server/app (->
                                  (mock/request :put "/events/aad96887-7ab9-43b7-a425-c9058c9e8e20")
                                  (mock/header "authorization" (str "Bearer " token))
                                  (mock/body (json/write-str updated-event))))]
            (is (= 201 (:status update-response)) "Event should be accepted with Created."))

          ; Also update a non-existent Event.
          (let [update-response (@server/app (->
                                  (mock/request :put "/events/aad96887-7ab9-43b7-a425-XXXXX")
                                  (mock/header "authorization" (str "Bearer " token))
                                  (mock/body (json/write-str updated-event))))]
            (is (= 403 (:status update-response)) "Non-existent Event should not be accepted."))

          ; Also try to update the Event without authorization.
          (let [update-response (@server/app (->
                                  (mock/request :put "/events/aad96887-7ab9-43b7-a425-XXXXX")
                                  (mock/header "authorization" (str "Bearer " "BAD TOKEN"))
                                  (mock/body (json/write-str updated-event))))]
            (is (= 403 (:status update-response)) "Bad auth token should not be accepted."))

          ; Now try and get the archive. It should have been deleted and re-created
          (let [response (@server/app (-> (mock/request :get (str "/events/archive/2016-11-25/aa"))
                                          (mock/header "authorization" (str "Bearer " @matching-token))))

                event (-> response :body (json/read-str :key-fn keyword) :events first)]

            (is (= updated-event (dissoc event :timestamp)) "Updated Event should be present in new Archive.")
            (is (not= event (dissoc event :timestamp)) "Updated Event is different to original Event.")))))))


; Other bits and pieces.

(deftest ^:component home-redirects
  (testing "URL base redirects to Event Data site."
    (let [response (@server/app
                     (mock/request :get "/"))]
      (is (= (get-in response [:status]) 302))
      (is (= (get-in response [:headers "Location"]) "https://www.crossref.org/services/event-data")))))

(deftest ^:component heartbeat-ok
  (testing "Heartbeat should return OK. This includes connection to external services."
    (let [response (@server/app
                     (mock/request :get "/heartbeat"))]
      (is (= (get-in response [:status]) 200)))))
