(ns event-data-event-bus.schema-tests
  "Tests for the schema namespace."
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [event-data-event-bus.schema :as schema]))


(deftest ^:unit schema
  (testing "Schema should exist and be verify inputs."
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
