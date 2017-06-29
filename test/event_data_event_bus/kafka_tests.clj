(ns event-data-event-bus.kafka-tests
  "Tests for the archive namespace.
   NOTE: The round-trip archive test via the API is tested in server-tests."
  (:require [clojure.test :refer :all]
            [event-data-event-bus.kafka :as kafka]
            [clojure.data.json :as json])
  (:import [com.auth0.jwt JWTSigner JWTVerifier]))


(def input
  "{\"license\":\"https:\\/\\/creativecommons.org\\/publicdomain\\/zero\\/1.0\\/\",\"obj_id\":\"https:\\/\\/doi.org\\/10.2307\\/604670\",\"source_token\":\"36c35e23-8757-4a9d-aacf-345e9b7eb50d\",\"occurred_at\":\"2017-06-22T18:26:00Z\",\"jwt\":\"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ3aWtpcGVkaWEifQ.xeDmPuUL8p-cEXc3pgz22z8sCWK5oEYU9Xqj20h9AJo\",\"subj_id\":\"https:\\/\\/en.wikipedia.org\\/w\\/index.php?title=Brahmi_script&oldid=786979483\",\"id\":\"85b0ffc2-4620-46c4-9495-26ef80b847cd\",\"evidence_record\":\"https:\\/\\/evidence.eventdata.crossref.org\\/evidence\\/evidence\\/20170622-wikipedia-e9f40ed8-7146-4df9-a5ee-7d28770e15ca\",\"action\":\"add\",\"subj\":{\"pid\":\"https:\\/\\/en.wikipedia.org\\/w\\/index.php?title=Brahmi_script&oldid=786979483\",\"url\":\"https:\\/\\/en.wikipedia.org\\/wiki\\/Brahmi_script\",\"title\":\"Brahmi script\",\"api-url\":\"https:\\/\\/en.wikipedia.org\\/api\\/rest_v1\\/page\\/html\\/Brahmi_script\\/786979483\"},\"source_id\":\"wikipedia\",\"obj\":{\"pid\":\"https:\\/\\/doi.org\\/10.2307\\/604670\",\"url\":\"https:\\/\\/doi.org\\/10.2307\\/604670\"},\"relation_type_id\":\"references\"}")

(def bad-input
  "Only difference is invalid JWT signature."
  "{\"license\":\"https:\\/\\/creativecommons.org\\/publicdomain\\/zero\\/1.0\\/\",\"obj_id\":\"https:\\/\\/doi.org\\/10.2307\\/604670\",\"source_token\":\"36c35e23-8757-4a9d-aacf-345e9b7eb50d\",\"occurred_at\":\"2017-06-22T18:26:00Z\",\"jwt\":\"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ3aWtpcGVkaWEifQ.BAD_SIGNATURE_CANT_VERIFY\",\"subj_id\":\"https:\\/\\/en.wikipedia.org\\/w\\/index.php?title=Brahmi_script&oldid=786979483\",\"id\":\"85b0ffc2-4620-46c4-9495-26ef80b847cd\",\"evidence_record\":\"https:\\/\\/evidence.eventdata.crossref.org\\/evidence\\/evidence\\/20170622-wikipedia-e9f40ed8-7146-4df9-a5ee-7d28770e15ca\",\"action\":\"add\",\"subj\":{\"pid\":\"https:\\/\\/en.wikipedia.org\\/w\\/index.php?title=Brahmi_script&oldid=786979483\",\"url\":\"https:\\/\\/en.wikipedia.org\\/wiki\\/Brahmi_script\",\"title\":\"Brahmi script\",\"api-url\":\"https:\\/\\/en.wikipedia.org\\/api\\/rest_v1\\/page\\/html\\/Brahmi_script\\/786979483\"},\"source_id\":\"wikipedia\",\"obj\":{\"pid\":\"https:\\/\\/doi.org\\/10.2307\\/604670\",\"url\":\"https:\\/\\/doi.org\\/10.2307\\/604670\"},\"relation_type_id\":\"references\"}")


(deftest ^:unit input-event->ring-request
  (testing "good input is transformed into Ring request"
    (let [result (kafka/input-event->ring-request input)]
      ; Need to test body separately as it's a Reader.
      (is (= (:request-method result) :post) "Request method should be set")
      (is (= (:jwt-claims result) {"sub" "wikipedia"}) "JWT claims should be verified and parsed.")
      (is (= (json/read (:body result) :key-fn keyword)
             (dissoc (json/read-str input :key-fn keyword) :jwt))
          "Body should be identical to input, but with JWT removed.")))

  (testing "claims for input with malfomed JWT is rejected aren't accepted"
    (let [result (kafka/input-event->ring-request bad-input)]
      (is (= (:request-method result) :post) "Request method should be set")
      (is (= (:jwt-claims result) nil) "JWT claims should not have been included."))))

