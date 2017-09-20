(ns event-data-event-bus.downstream-tests
  "Tests for the downstream namspace.
   The unit tests use supplied configuration maps,
   the component tests rely on configuration variables
   set through the docker-compose-component-test.yml"
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [config.core :refer [env]]
            [event-data-event-bus.downstream :as downstream]
            [org.httpkit.fake :as fake]))

; Config in docker-compose-compoennt-tests.yml sets configuration.

(deftest ^:component parse-environment-variables
  (testing "load-broadcast-config can read an environment variable configuration structure from real environment variables and produce a downstream configuration."
    (is (= {:http-post-live {
             :example.com_post {
               :jwt "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyIxIjoiMSIsInN1YiI6Indpa2lwZWRpYSJ9.w7zV2vtKNzrNDfgr9dfRpv6XYnspILRli_V5vd1J29Q"
               :endpoint "http://example.com/events"}
               :example2.com_post {
                :jwt "XXX"
                :endpoint "http://example2.com/events"}}
            :kafka-live {
              :my_kafka {
                :bootstrap-servers "localhost:9092,other:9092"
                :topic "my-topic"}
              :your_kafka {
                :bootstrap-servers "you:9092"
                :topic "your-topic"}}}

           (downstream/load-broadcast-config)) "Correct structure should be retrieved ")
    (is (= @downstream/downstream-config-cache (downstream/load-broadcast-config)) "Config should be cached.")))

(deftest ^:component incoming-outgoing
  (testing "All incoming events should be sent to all listeners, even unreliable ones."
    ; We have unreliable downstream agents.
    (let [http-endpoint-1-i (atom -1)
          http-endpoint-2-i (atom -1)

          http-good-response {:status 201 :body "Good!"}
          http-bad-response {:status 400 :body "Bad?"}
          http-error-response {:status 500 :body "Ugly‽"}

          ; A selection of unreliable, but ultimately functional downstream consumers.
          http-endpoint-1-responses [http-bad-response http-error-response http-good-response]
          http-endpoint-2-responses [http-error-response http-bad-response http-good-response]

          ; We'll need to block the test because this is asynchronous.
          http-endpoint-1-done (promise)
          http-endpoint-2-done (promise)

          ; Collect the bodies that are sent.
          http-sent-1 (atom (list))
          http-sent-2 (atom (list))

          ; Two monitors so our test can wait until everything's finished running.
          http-monitor-1 (promise)
          http-monitor-2 (promise)

          kafka-sent-1 (atom nil)
          kafka-sent-2 (atom nil)
          kafka-monitor-1 (promise)
          kafka-monitor-2 (promise)
          kafka-fake-producer :fake]

      ; Fake HTTP endpoints that step through the sequence of responses.
      (fake/with-fake-http ["http://example.com/events"
                            (fn [orig-fn opts callback] 
                              ; Increment counter.
                              (swap! http-endpoint-1-i inc)
                              
                              ; When we reach the end, deliver the promise so that the test can stop waiting.
                              (when (= http-good-response (http-endpoint-1-responses @http-endpoint-1-i))
                                ; Save the body that was sent on a successful response.
                                (swap! http-sent-1 #(conj % (:body opts)))
                                (deliver http-monitor-1 1))

                              ; Return the predetermined response per counter.
                              (nth http-endpoint-1-responses @http-endpoint-1-i))


                            "http://example2.com/events"
                            (fn [orig-fn opts callback] 

                              ; Increment counter.
                              (swap! http-endpoint-2-i inc)

                              ; When we reach the end, deliver the promise so that the test can stop waiting.
                              (when (= http-good-response (http-endpoint-2-responses @http-endpoint-2-i))
                                ; Save the body that was sent on a successful response.
                                (swap! http-sent-2 #(conj % (:body opts)))
                                (deliver http-monitor-2 1))

                              ; Return the predetermined response per counter.
                              (nth http-endpoint-2-responses @http-endpoint-2-i))]

        ; Also fake out Kafka sending.
        (with-redefs [event-data-event-bus.downstream/retry-delay 0

                      ; Prevent producer from being constructed.
                      ; We're mocking out the only thing that uses it anyway.
                      event-data-event-bus.downstream/get-kafka-producer
                      (constantly kafka-fake-producer)

                      event-data-event-bus.downstream/send-kafka-producer
                      (fn [producer topic id event]
                        (when (= topic "my-topic")
                          (swap! kafka-sent-1 #(conj % event))
                          (deliver kafka-monitor-1 1))

                        (when (= topic "your-topic")
                          (swap! kafka-sent-2 #(conj % event))
                          (deliver kafka-monitor-2 1)))]

          ; Broadcast using the configuration from environment variables and see what happens.
          ; A tiny Event with only an ID. Not technically valid, but enough to test.
          (downstream/broadcast-live {:id "d24e5449-7835-44f4-b7e6-289da4900cd0"}))

        ; Wait for everything to complete.
        @http-monitor-1
        @http-monitor-2
        @kafka-monitor-1
        @kafka-monitor-2

        (is (= ["{\"id\":\"d24e5449-7835-44f4-b7e6-289da4900cd0\"}"] @http-sent-1)
            "Correct body should be sent to HTTP endpoint 1 exactly once.")
        
        (is (= ["{\"id\":\"d24e5449-7835-44f4-b7e6-289da4900cd0\"}"] @http-sent-2)
            "Correct body should be sent to HTTP endpoint 2 exactly once.")

        (is (= ["{\"id\":\"d24e5449-7835-44f4-b7e6-289da4900cd0\"}"] @kafka-sent-1)
            "Correct body should be sent to Kafka connection 1 exactly once.")

        (is (= ["{\"id\":\"d24e5449-7835-44f4-b7e6-289da4900cd0\"}"] @kafka-sent-2)
            "Correct body should be sent to Kafka connection 2 exactly once.")))))


