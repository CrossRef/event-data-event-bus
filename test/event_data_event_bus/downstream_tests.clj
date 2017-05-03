(ns event-data-event-bus.downstream-tests
  "Tests for the downstream namspace.
   The unit tests use supplied configuraiton maps,
   the component tests rely on configuration variables
   set through the docker-compose-component-test.yml"
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [config.core :refer [env]]
            [event-data-event-bus.downstream :as downstream]
            [org.httpkit.fake :as fake]))

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

   :broadcast-myqueue-username "myusername"
   :broadcast-myqueue-password "mypassword"
   :broadcast-myqueue-endpoint "tcp://my-active-mq-endpoint:61616"
   :broadcast-myqueue-queue "my-queue"
   :broadcast-myqueue-name "ActiveMQ Query API"
   :broadcast-myqueue-type "activemq-queue"

   :completely-irrelevant "don't look"
   :other-config-keys "turn away"
   :broadcast-something-else "igonre me"})

(deftest ^:unit parse-broadcast-config
  (let [endpoint-missing (dissoc good-config-map :broadcast-2-endpoint)
        name-missing (dissoc good-config-map :broadcast-3-name)
        type-missing (dissoc good-config-map :broadcast-datacite-type)
        invalid-type (assoc good-config-map :broadcast-1-type "platypus")
        good-result (downstream/parse-broadcast-config good-config-map)]

    (is (= (dissoc good-result :activemq-queue)
            {:live
              (set [{:label "1" :type "live" :jwt "JWT_ONE" :endpoint "http://one.com/endpoint" :name "Endpoint Number One"}
                    {:label "3" :type "live" :jwt "JWT_THREE" :endpoint "http://three.com/endpoint" :name "Endpoint Number Three"}])
             :batch
              (set [{:label "2" :type "batch" :jwt "JWT_TWO" :endpoint "http://two.com/endpoint" :name "Endpoint Number Two"}
                    {:label "datacite" :type "batch" :jwt "JWT_FOR_DATACITE" :endpoint "http://datcite.org/endpoint" :name "Endpoint for DataCite"}])}))

    ; Factory doesn't implement equality. That's probably a good thing.
    (is (= (-> good-result :activemq-queue first (dissoc :connection))
           {:label "myqueue" :type "activemq-queue" :queue "my-queue" :name "ActiveMQ Query API" :password "mypassword" :endpoint "tcp://my-active-mq-endpoint:61616" :username "myusername"}))

    (testing "parse-broadcast-config is able to parse a downstream configuration out of a configuration map"
      (is (nil? (downstream/parse-broadcast-config endpoint-missing)) "Missing endpoint key in one item should result in error.")
      (is (nil? (downstream/parse-broadcast-config name-missing)) "Missing name key in one item should result in error.")
      (is (nil? (downstream/parse-broadcast-config type-missing)) "Missing type key in one item should result in error.")
      (is (nil? (downstream/parse-broadcast-config invalid-type)) "Invalid type in one item should result in error."))))

(deftest ^:component parse-environment-variables
  (testing "load-broadcast-config can read an environment variable configuration structure from real environment variables and produce a downstream configuration."
    (is (= {:live #{{:label "1", :type "live", :name "Endpoint One", :endpoint "http://endpoint1.com/endpoint", :jwt "JWT-ONE"}
                    {:label "2", :type "live", :name "Endpoint Two", :endpoint "http://endpoint2.com/endpoint", :jwt "JWT-TWO"}},
            :batch #{}
            :activemq-queue #{}}
           (downstream/load-broadcast-config)) "Correct structure should be retrieved ")
    (is (= @downstream/downstream-config-cache (downstream/load-broadcast-config)) "Config should be cached.")))

(deftest ^:component incoming-outgoing
  (testing "All incoming events should be sent to all listeners, even unreliable ones."
    ; Pre-check that we got what we expected.
    (is (= {:live #{{:label "1", :name "Endpoint One", :endpoint "http://endpoint1.com/endpoint", :jwt "JWT-ONE", :type "live"}
                    {:label "2", :type "live", :name "Endpoint Two", :jwt "JWT-TWO", :endpoint "http://endpoint2.com/endpoint"}},
            :batch #{}
            :activemq-queue #{}}
           (downstream/load-broadcast-config)) "Correct structure should be retrieved ")
      
      ; We have unreliable downstream agents.
      (let [endpoint-1-i (atom -1)
            endpoint-2-i (atom -1)

            good-response {:status 201 :body "Good!"}
            bad-response {:status 400 :body "Bad?"}
            error-response {:status 500 :body "Ugly‽"}

            ; A selection of unreliable, but ultimately functional downstream consumers.
            endpoint-1-responses [bad-response error-response good-response]
            endpoint-2-responses [error-response bad-response good-response]

            ; We'll need to block the test because this is asynchronous.
            endpoint-1-done (promise)
            endpoint-2-done (promise)

            ; Collect the bodies that are sent.
            sent-1 (atom (list))
            sent-2 (atom (list))

            ; Two monitors so our test can wait until everything's finished running.
            monitor-1 (promise)
            monitor-2 (promise)]

        ; Fake endpoints that step through the sequence of responses.
        (fake/with-fake-http ["http://endpoint1.com/endpoint"
                              (fn [orig-fn opts callback] 
                                ; Increment counter.
                                (swap! endpoint-1-i inc)
                                
                                ; When we reach the end, deliver the promise so that the test can stop waiting.
                                (when (= good-response (endpoint-1-responses @endpoint-1-i))
                                  ; Save the body that was sent on a successful response.
                                  (swap! sent-1 #(conj % (:body opts)))
                                  (deliver monitor-1 1))

                                ; Return the predetermined response per counter.
                                (nth endpoint-1-responses @endpoint-1-i))


                              "http://endpoint2.com/endpoint"
                              (fn [orig-fn opts callback] 

                                ; Increment counter.
                                (swap! endpoint-2-i inc)

                                ; When we reach the end, deliver the promise so that the test can stop waiting.
                                (when (= good-response (endpoint-2-responses @endpoint-2-i))
                                  ; Save the body that was sent on a successful response.
                                  (swap! sent-2 #(conj % (:body opts)))
                                  (deliver monitor-2 1))

                                ; Return the predetermined response per counter.
                                (nth endpoint-2-responses @endpoint-2-i))]


          ; Broadcast using the configuration from environment variables and see what happens.
          ; A tiny Event with only an ID. Not technically valid, but enough to test.
          (downstream/broadcast-live {:id "d24e5449-7835-44f4-b7e6-289da4900cd0"})

          ; Wait for everything to complete.
          @monitor-1
          @monitor-2

          (is (= ["{\"id\":\"d24e5449-7835-44f4-b7e6-289da4900cd0\"}"] @sent-1) "Correct body should be sent to endpoint 1 exactly once.")
          (is (= ["{\"id\":\"d24e5449-7835-44f4-b7e6-289da4900cd0\"}"] @sent-2) "Correct body should be sent to endpoint 2 exactly once.")))))

