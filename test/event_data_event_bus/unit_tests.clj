(ns event-data-event-bus.unit-tests
  "Unit tests for individual functions."
  (:require [clojure.test :refer :all]
            [event-data-event-bus.server :as server]
            [event-data-event-bus.schema :as schema]
            [clojure.data.json :as json])
  (:import [com.auth0.jwt JWTSigner JWTVerifier]))

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

(deftest ^:unit timestamp-event
  (testing "Events can be timestamped"
    (let [original-event {:contents :do :not :matter}
          [timestamped-event yyyy-mm-dd] (server/timestamp-event original-event)]

      (is (not (:timestamp original-event)) "Pre: timestamp not in original event.")

      (is (clojure.set/subset? (set original-event) (set timestamped-event)) "Original keys and values should be present.")
      (is (:timestamp timestamped-event) "Timestamp added to event")
      (is yyyy-mm-dd "YYYY-MM-DD returned")
      (is (.startsWith (:timestamp timestamped-event) yyyy-mm-dd) "YYYY-MM-DD is consistent with timestamp"))))

(deftest ^:unit schema
  (testing "Schema should exist and"
    (is @schema/schema "It should be possible to load the schema"))
  
  (let [good-structure {"obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
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
                        "source_id" "the-source-id"
                        "relation_type_id" "references"}
        good-json (json/write-str good-structure)

        bad-structure {"obj_id" "https://doi.org/10.1093/EMBOJ/20.15.4132"
                       ; occurred_at is missing
                       ; subj_id is missing
                       "total" 1
                       "id" "EVENT-ID-1"
                       "subj" {
                         "pid" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                         "title" "Señalización paracrina"
                         "issued" "2016-09-25T23:58:58.000Z"
                         "URL" "https://es.wikipedia.org/wiki/Se%C3%B1alizaci%C3%B3n_paracrina"
                         "type" "entry-encyclopedia"}
                       "message_action" "create"
                       "source_id" "the-source-id"
                       "relation_type_id" "references"}

        bad-json (json/write-str bad-structure)]
 
  (testing "Schema validates input both as structure and JSON."
    (is (nil? (schema/validation-errors good-structure)) "Good Event structure should pass schema.")
    (is (nil? (schema/json-validation-errors good-json)) "Good Event JSON should pass schema.")
    
    (let [bad-results (schema/validation-errors bad-structure)
          bad-json-results (schema/json-validation-errors bad-json)]

      (is bad-results "Bad Event structure should not pass schema.")
      (is bad-json-results "Bad Event JSON should not pass schema.")

      (is (= (set ["subj_id" "occurred_at"])
             (set (-> bad-results first :missing))) "Missing field should be identified from structure")

      (is (= (set ["subj_id" "occurred_at"])
             (set (-> bad-json-results first :missing))) "Missing field should be identified from JSON")))))


