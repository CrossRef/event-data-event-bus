(ns event-data-event-bus.downstream-tests
  "Tests for the downstream namspace.
   The unit tests use supplied configuraiton maps,
   the component tests rely on configuration variables
   set through the docker-compose-component-test.yml"
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [config.core :refer [env]]
            [event-data-event-bus.downstream :as downstream]))

(def good-config-map
  "A config map such as that supplied by config.core"
  {:broadcast-1-jwt "JWT_ONE"
   :broadcast-1-endpoint "http://one.com/endpoint"
   :broadcast-1-name "Endpoint Number One"
   :broadcast-1-type "live"

   :broadcast-2-jwt "JWT_TWO"
   :broadcast-2-endpoint "http://two.com/endpoint"
   :broadcast-2-name "Endpoint Number Two"
   :broadcast-2-type "batch"

   :broadcast-3-jwt "JWT_THREE"
   :broadcast-3-endpoint "http://three.com/endpoint"
   :broadcast-3-name "Endpoint Number Three"
   :broadcast-3-type "live"

   :broadcast-datacite-jwt "JWT_FOR_DATACITE"
   :broadcast-datacite-endpoint "http://datcite.org/endpoint"
   :broadcast-datacite-name "Endpoint for DataCite"
   :broadcast-datacite-type "batch"


   :completely-irrelevant "don't look"
   :other-config-keys "turn away"
   :broadcast-something-else "igonre me"})

(deftest ^:unit parse-broadcast-config
  (let [; a JWT missing.
        jwt-missing (dissoc good-config-map :broadcast-1-jwt)
        endpoint-missing (dissoc good-config-map :broadcast-2-endpoint)
        name-missing (dissoc good-config-map :broadcast-3-name)
        type-misisng (dissoc good-config-map :broadcast-datacite-type)
        invalid-type (assoc good-config-map :broadcast-1-type "platypus")]

    (is (= (downstream/parse-broadcast-config good-config-map)
            {:live
              (set [{:label "1" :type "live" :jwt "JWT_ONE" :endpoint "http://one.com/endpoint" :name "Endpoint Number One"}
                    {:label "3" :type "live" :jwt "JWT_THREE" :endpoint "http://three.com/endpoint" :name "Endpoint Number Three"}])
             :batch
              (set [{:label "2" :type "batch" :jwt "JWT_TWO" :endpoint "http://two.com/endpoint" :name "Endpoint Number Two"}
                    {:label "datacite" :type "batch" :jwt "JWT_FOR_DATACITE" :endpoint "http://datcite.org/endpoint" :name "Endpoint for DataCite"}])})
        "Valid configuration should return correct configuration structure.")

    (testing "parse-broadcast-config is able to parse a downstream configuration out of a configuration map"
      (is (nil? (downstream/parse-broadcast-config jwt-missing)) "Missing jwt key in one item should result in error.")
      (is (nil? (downstream/parse-broadcast-config endpoint-missing)) "Missing endpoint key in one item should result in error.")
      (is (nil? (downstream/parse-broadcast-config name-missing)) "Missing name key in one item should result in error.")
      (is (nil? (downstream/parse-broadcast-config type-misisng)) "Missing type key in one item should result in error.")
      (is (nil? (downstream/parse-broadcast-config invalid-type)) "Invalid type in one item should result in error."))))

(deftest ^:component parse-environment-variables
  (testing "load-broadcast-config can read an environment variable configuration structure from real environment variables and produce a downstream configuration."
    (is (= {:live #{{:label "1", :name "Endpoint One", :endpoint "http://endpoint1.com/endpoint", :jwt "JWT-ONE", :type "live"}},
            :batch #{{:label "2", :type "batch", :name "Endpoint Two", :jwt "JWT-TWO", :endpoint "http://endpoint2.com/endpoint"}}}
           (downstream/load-broadcast-config)) "Correct structure should be retrieved ")
    (is (= @downstream/downstream-config-cache (downstream/load-broadcast-config)) "Config should be cached.")))





