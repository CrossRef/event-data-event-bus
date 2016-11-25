(ns event-data-event-bus.schema
  "Validate events according to the Event Data schema.
   Two validators, one for JSON strings and one for pre-parsed Clojure map structures."
  (:require [clojure.java.io :as io])
  (:require [scjsv.core :as scjsv]
            [clojure.data.json :as json]))

(defn load-json-schema []
  (-> "schema.json" io/resource io/input-stream io/reader json/read scjsv/json-validator))

(def json-schema
  (delay (load-json-schema)))

(defn load-schema []
  (-> "schema.json" io/resource io/input-stream io/reader json/read scjsv/validator))

(def schema
  (delay (load-schema)))


(defn validation-errors
  "Validation errors from Clojure structure."
  [structure]
  (@schema structure))

(defn json-validation-errors
  "Validation errors from JSON string."
  [json-input]
  (@json-schema json-input))

