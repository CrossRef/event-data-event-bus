(ns event-data-event-bus.api-tests
  "Component tests that work at the Ring level, including routing.
   These run through all the middleware including JWT extraction."
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [event-data-event-bus.server :as server]
            [ring.mock.request :as mock])
  (:import [com.auth0.jwt JWTSigner]))

(def signer
  "A JWT signer that uses a secret we expect."
  (delay (new JWTSigner "TEST")))

(def unrecognised-signer
  "A JWT signer that uses a secret we don't expect."
  (delay (new JWTSigner "UNRECOGNISED")))

(def source-id "1111111111")

(def good-event {
  "obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
  "occurred_at" "2016-09-25T23:58:58Z"
  "subj_id" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
  "total" 1
  "id" "d24e5449-7835-44f4-b7e6-289da4900cd0"
  "subj" {
    "pid" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
    "title" "Señalización paracrina"
    "issued" "2016-09-25T23:58:58.000Z"
    "URL" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
    "type" "entry-encyclopedia"
  }
  "message_action" "create"
  "source_id" source-id
  "relation_type_id" "references"})

(def good-event-json (delay (json/write-str good-event)))

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

(deftest ^:component should-accept-good-events
  (testing "Submitted events with correctly signed JWT that matches the `subj` of the `source_id` should be accepted."
    (let [token (.sign @signer {"sub" source-id})
          request (->
              (mock/request :post "/events")
              (mock/header "authorization" (str "Bearer " @matching-token))
              (mock/body @good-event-json))
          response (@server/app request)]
      (is (= 201 (:status response)) "Should be created OK."))))

(deftest ^:component should-reject-events-not-matching-token
  (testing "Submitted events with correctly signed JWT but non-matching `subj` of the `source_id` should be rejected."
    (let [token (.sign @signer {"sub" "my-service"})
          request (->
              (mock/request :post "/events")
              (mock/header "authorization" (str "Bearer " @not-matching-token))
              (mock/body @good-event-json))
          response (@server/app request)]
      (is (= 403 (:status response)) "Should be forbidden"))))

(deftest ^:component should-reject-unsigned-events
  (testing "Submitted events with incorrect JWT should be accepted."
    (let [token (.sign @signer {"sub" "my-service"})
          request (->
              (mock/request :post "/events")
              (mock/header "authorization" (str "Bearer " @not-matching-token))
              (mock/body @good-event-json))
          response (@server/app request)]
      (is (= 403 (:status response)) "Should be forbidden"))))

(deftest ^:component home-redirects
  (testing "URL base redirects to Event Data site."
    (let [response (@server/app
                     (mock/request :get "/"))]
      (is (= (get-in response [:status]) 302))
      (is (= (get-in response [:headers "Location"]) "http://eventdata.crossref.org/")))))

(deftest ^:component heartbeat-ok
  (testing "Heartbeat should return OK. This includes connection to external services."
    (let [response (@server/app
                     (mock/request :get "/heartbeat"))]
      (is (= (get-in response [:status]) 200)))))


(deftest ^:component should-reject-duplicate-events
  ; TODO
)

(deftest ^:component should-assign-timestamps
  ; TODO
)
