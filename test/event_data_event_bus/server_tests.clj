(ns event-data-event-bus.server-tests
  "Tests for the server namespace.
   Most component tests work at the Ring level, including routing.
   These run through all the middleware including JWT extraction."
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [event-data-event-bus.server :as server]
            [ring.mock.request :as mock])
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

; Authentication

(deftest ^:unit get-token
  (testing "Get-token should retrieve the token from a request."
    (is (= "abcdefgh" (server/get-token
                        {:remote-addr "172.17.0.1",
                         :headers {
                           "accept" "*/*",
                           "authorization" "Bearer abcdefgh",
                           "host" "localhost:9990",
                           "user-agent" "curl/7.49.1"},
                         :uri "/heartbeat",
                         :server-name "localhost",
                         :body nil,
                         :scheme :http,
                         :request-method :get}))))

  (testing "Get-token should return nil if not present."
    (is (= nil (server/get-token
                {:remote-addr "172.17.0.1",
                 :headers {
                   "accept" "*/*",
                   "authorization" "",
                   "host" "localhost:9990",
                   "user-agent" "curl/7.49.1"},
                 :uri "/heartbeat",
                 :server-name "localhost",
                 :body nil,
                 :scheme :http,
               :request-method :get}))))

    (testing "Get-token should return nil if malformed."
      (is (= nil (server/get-token 
                  {:remote-addr "172.17.0.1",
                   :headers {
                     "accept" "*/*",
                     "authorization" "Token abcdefgh",
                     "host" "localhost:9990",
                     "user-agent" "curl/7.49.1"},
                   :uri "/heartbeat",
                   :server-name "localhost",
                   :body nil,
                   :scheme :http,
                   :request-method :get})))))

(deftest ^:unit wrap-jwt
  (let [jwt-secret1 "TEST"
        jwt-secret2 "TEST2"
        middleware (server/wrap-jwt identity "TEST,TEST2")
        signer1 (new JWTSigner jwt-secret1)
        signer2 (new JWTSigner jwt-secret2)
        token-content {"test-key" "test-value"}
        token1 (.sign signer1 token-content)
        token2 (.sign signer2 token-content)]

  (testing "wrap-jwt should decode more than one token"
    (let [wrapped1 (middleware
                      {:remote-addr "172.17.0.1"
                       :headers {
                         "accept" "*/*"
                         "authorization" (str "Bearer " token1)
                         "host" "localhost:9990"
                         "user-agent" "curl/7.49.1"}
                       :uri "/heartbeat"
                       :server-name "localhost"
                       :body nil
                       :scheme :http
                       :request-method :get})

          wrapped2 (middleware
                      {:remote-addr "172.17.0.1"
                       :headers {
                         "accept" "*/*"
                         "authorization" (str "Bearer " token2)
                         "host" "localhost:9990"
                         "user-agent" "curl/7.49.1"}
                       :uri "/heartbeat"
                       :server-name "localhost"
                       :body nil
                       :scheme :http
                       :request-method :get})

          wrapped-bad (middleware
                      {:remote-addr "172.17.0.1"
                       :headers {
                         "accept" "*/*"
                         "authorization" (str "Bearer BAD TOKEN")
                         "host" "localhost:9990"
                         "user-agent" "curl/7.49.1"}
                       :uri "/heartbeat"
                       :server-name "localhost"
                       :body nil
                       :scheme :http
                       :request-method :get})]

    (is
      (= token-content (:jwt-claims wrapped1))
      "Token can be decoded using one of the secrets, associated to request.")

    (is
      (= token-content (:jwt-claims wrapped2))
      "Token can be decoded using the other of the secrets, associated to request.")

    (is
      (= nil (:jwt-claims wrapped-bad))
      "Invalid token ignored.")))))

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
    (let [event   {"obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
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
          get-event-response (@server/app (mock/request :get "/events/EVENT-ID-1"))]

      (is (= 201 (:status response)) "Should be created OK.")
      ; The returned event will have added field(s) e.g. timestamp, so won't be identical. Should be a proper superset though.
      (is (clojure.set/subset? (set event)
                               (set (json/read-str (:body get-event-response))))) "Same event should be retrievable afterwards.")))

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
      (is (= 403 (:status second-response)) "Duplicate should be rejected.")))

(deftest ^:component should-assign-timestamps
  ; TODO
)

; Other bits and pieces.

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

