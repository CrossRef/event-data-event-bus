(ns event-data-event-bus.server
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.server :as server]
            [config.core :refer [env]]
            [compojure.core :refer [defroutes GET POST PUT]]
            [ring.util.response :as ring-response]
            [ring.middleware.params :as middleware-params]
            [ring.middleware.content-type :as middleware-content-type]
            [liberator.core :refer [defresource]]
            [liberator.representation :as representation]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce]
            [clojure.java.io :refer [reader input-stream]]
            [event-data-common.jwt :as jwt]
            [event-data-common.storage.redis :as redis]
            [event-data-common.storage.s3 :as s3]
            [event-data-common.storage.memory :as memory]
            [event-data-common.storage.store :as store]
            [event-data-common.date :as date]
            [event-data-common.evidence-log :as evidence-log]
            [event-data-event-bus.schema :as schema]
            [event-data-event-bus.archive :as archive]
            [event-data-event-bus.downstream :as downstream]
            [overtone.at-at :as at-at])
  (:import
           [java.net URL MalformedURLException InetAddress])
  (:gen-class))

(def event-data-homepage "https://www.crossref.org/services/event-data")

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(def quick-check-expiry
  "How long in milliseconds before mutex for quick-check expires? Should cover any delay in S3 propagagion."
  (*
    1000 ; 1 seconds
    60 ; 1 minute
    10 ; 10 minutes
))

(def redis-prefix
  "Unique prefix applied to every key."
  "event-data-bus:")

(def up-since
  "Start the clock so we know approximately how long this instance has been running."
  (delay (clj-time/now)))

(def default-redis-db-str "0")

(def redis-store
  "A redis connection for storing subscription and short-term information."
  (delay (redis/build
            redis-prefix
            (:bus-redis-host env)
            (Integer/parseInt (:bus-redis-port env))
            (Integer/parseInt (get env :bus-redis-db default-redis-db-str)))))

(def storage
  "A event-data-event-bus.storage.store.Store for permanently storing things"
  (delay
    (condp = (:bus-storage env "s3")
      ; Memory can be used for unit testing ONLY.
      "memory" (memory/build)
      ; Redis can be used for component testing ONLY. Reuse the redis connection.
      "redis" @redis-store
      ; S3 is suitable for production.
      "s3" (s3/build (:bus-s3-key env)
                     (:bus-s3-secret env)
                     (:bus-s3-region-name env)
                     (:bus-s3-bucket-name env)))))

(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
                (representation/ring-response
                  (ring-response/redirect event-data-homepage))))

;  "Expose heartbeat."
(defresource heartbeat
  []
  :allowed-methods [:get]
  :available-media-types ["application/json"]
  :handle-ok (fn [context]
              (let [now (clj-time/now)]
                (let [report {:machine_name (.getHostName (InetAddress/getLocalHost))
                              :version (System/getProperty "event-data-event-bus.version")
                              :up-since (str @up-since)
                              :now (str now)
                              :status "OK"}]
                  report))))

;   "Convenience method for checking JWTs."

(defresource auth-test
  []
  :allowed-methods [:get]
  :available-media-types ["application/json"]
  :authorized? (fn [ctx]
                ; sub must be supplied to post.
                (-> ctx :request :jwt-claims :sub))
  :handle-ok {"status" "ok"})

(defn timestamp-event
  "Prepare an Event for ingestion. Add timestamp.
   Return [event, 'YYYY-MM-DD']"
  [event]
  (let [now (clj-time/now)
        iso8601 (clj-time-format/unparse date-format now)
        yyyy-mm-dd (clj-time-format/unparse date/yyyy-mm-dd-format now)]
    [(assoc event :timestamp iso8601) yyyy-mm-dd]))

(defn broadcast-live-async
  [event]
  (future
    (downstream/broadcast-live event)))

; "Create Events."
(defresource events
  []
  :allowed-methods [:post]
  :available-media-types ["application/json"]
  :authorized? (fn
                [ctx]
                ; Authorized if the JWT claims are correctly signed.
                (-> ctx :request :jwt-claims))

  :malformed? (fn [ctx]
                (let [event (try (-> ctx :request :body reader (json/read :key-fn keyword)) (catch Exception _ nil))
                      schema-errors (schema/validation-errors event)]
                  [schema-errors {::event event ::schema-errors schema-errors}]))

  :allowed? (fn
              [ctx]
              ; Allowed only if the `sub` of the claim matches the `source_id` of the event.
              (let [; Check that the source ID matches.
                    source-id-ok (= (-> ctx ::event :source_id)
                                    (get-in ctx [:request :jwt-claims "sub"]))
                    
                    event-id (-> ctx ::event :id)
                    
                    ; Get the event with timestamp.
                    [event-with-timestamp yyyy-mm-dd] (timestamp-event (::event ctx))]

                  [source-id-ok {::event-id event-id
                                 ; Replace Event in ctx with timestamped one.
                                 ::event event-with-timestamp
                                 ::yyyy-mm-dd yyyy-mm-dd}]))
  
  ; This combination of :put-to-existing? true, :conflict? true and :post-to-existing? checking pre-existence
  ; has the effect of returning a 409 conflict when a duplicate Event is sent.
  ; Slighly odd but these are logic that Liberator uses.
  :put-to-existing? true
  :conflict? true

  :post-to-existing?
  (fn [ctx]
    (let [event-id (-> ctx ::event :id)
      
          ; Do a quick check in Redis to see if the event already exists.
          ; The mutex expires after a bit, long enough to cover any delay in S3 propagation.
          quick-check-ok (redis/expiring-mutex!? @redis-store (str "exists" event-id) quick-check-expiry)

          ; Also check permanent storage for existence.
          storage-check-ok (nil? (store/get-string @storage (archive/storage-path-for-event event-id)))]
      
      (and quick-check-ok storage-check-ok)))

  :handle-malformed (fn [ctx]
                      (json/write-str (if-let [schema-errors (::schema-errors ctx)]
                        {:status "Malformed"
                         :schema-errors schema-errors}
                        {:status "Malformed"})))

  :post! (fn [ctx]
    ; Don't return the URL of the new event on the API (although it should be available),
    ; because the Event Bus API doesn't guarantee read-after-write.
    ; We still check the `event/«id»` endpoint for component testing though.
    (let [json (json/write-str (::event ctx))]
      (evidence-log/log!
        {:i "b0001"
         :s "event-bus"
         :c "event"
         :f "received"
         :n (-> ctx ::event :id)
         :v (-> ctx ::event :source_id)})

        ; Send to all subscribers.
        (broadcast-live-async (::event ctx))

      ; And save.
      (archive/save-event @storage (::event-id ctx) (::yyyy-mm-dd ctx) json))))

(defresource update-event
  [id]
  :allowed-methods [:put]
  :available-media-types ["application/json"]

  :malformed? (fn [ctx]
                (let [payload (try (-> ctx :request :body reader (json/read :key-fn keyword)) (catch Exception _ nil))
                      schema-errors (schema/validation-errors payload)]
                  [schema-errors {::payload payload ::schema-errors schema-errors}]))

  :allowed? (fn [ctx]
              (let [event-str (store/get-string @storage (str archive/event-prefix id))
                    event-exists? (not (nil? event-str))
                    old-event (when event-exists? (json/read-str event-str :key-fn keyword))

                    ; The update can only happen if the sender has a valid claim for the source_id.
                    source-id-ok (= (:source_id old-event)
                                    (get-in ctx [:request :jwt-claims "sub"]))

                    merged-event (assoc (::payload ctx) :timestamp (:timestamp old-event))]

                [(and event-exists? source-id-ok) {::new-event merged-event}]))

    :put! (fn [ctx]
                  (let [new-event (::new-event ctx)
                        json (json/write-str new-event)
                        date-str (.substring (:timestamp new-event) 0 10)]
                    
                    (evidence-log/log!
                      {:i "b0002"
                       :s "event-bus"
                       :c "event"
                       :f "updated"
                       :n (-> new-event :id)
                       :v (-> new-event :source_id)})


                    ; Send to all subscribers.
                    (broadcast-live-async new-event)

                    ; And save.
                    (archive/save-event @storage (:id new-event) date-str json)

                    ; And remove the archive that contains this Event.
                    (archive/invalidate-archive-for-event-id @storage (:id new-event) date-str))))

(defresource event
  [id]
  :allowed-methods [:get]
  :available-media-types ["application/json"]

  :exists? (fn [ctx]
    (let [event (store/get-string @storage (str archive/event-prefix id))
          event-exists? (not (nil? event))]
      [event-exists? {::event event}]))
  :handle-ok (fn [ctx]
    (::event ctx)))

; A live-generated archive. Not for routine use.
(defresource events-archive
  [date-str prefix]
  :available-media-types ["application/json"]

  :authorized? (fn [ctx]
                ; Authorized if the JWT claims are correctly signed.
                (-> ctx :request :jwt-claims))

  :malformed? (fn [ctx]
    (let [date (try (clj-time-format/parse date/yyyy-mm-dd-format date-str) (catch IllegalArgumentException _ nil))]
      [(nil? date) {::date date}]))
  :allowed? (fn [ctx]
              ; The date argument specifies the start of the day for which we want events.
              ; Only allowed if the requested date range is yesterday or before.
              (let [; Find the end of the day queried.
                    end-date (clj-time/plus (::date ctx) (clj-time/days 1))

                    ; The latest it can be is midnight at the start of today (i.e. end of yesterday).
                    ; Add an extra hour to allow storage to propagate.
                    latest (clj-time/minus (clj-time/now) (clj-time/hours 1))

                    allowed (clj-time/before? end-date latest)]
                allowed))
  :handle-ok (fn [ctx]
                (archive/get-or-generate-archive @storage date-str prefix)))

(defresource events-ids
  [date-str prefix]
  :available-media-types ["application/json"]

  :authorized? (fn [ctx]
                ; Authorized if the JWT claims are correctly signed.
                (-> ctx :request :jwt-claims))

  :malformed? (fn [ctx]
    (let [date (try (clj-time-format/parse date/yyyy-mm-dd-format date-str) (catch IllegalArgumentException _ nil))]
      [(nil? date) {::date date}]))
  :allowed? (fn [ctx]
              ; The date argument specifies the start of the day for which we want events.
              ; Only allowed if the requested date range is yesterday or before.
              (let [; Find the end of the day queried.
                    end-date (clj-time/plus (::date ctx) (clj-time/days 1))

                    ; The latest it can be is midnight at the start of today (i.e. end of yesterday).
                    ; Add an extra hour to allow storage to propagate.
                    latest (clj-time/minus (clj-time/now) (clj-time/hours 1))

                    allowed (clj-time/before? end-date latest)]
                allowed))
  :handle-ok (fn [ctx]
                {"archive-generated" (clj-time-format/unparse date-format (clj-time/now))
                 "date" date-str
                 "prefix" prefix
                 "event-ids" (archive/event-ids-for @storage date-str prefix)}))

(defroutes app-routes
  (GET "/" [] (home))
  (POST "/events" [] (events))
  (GET "/events/archive/:date/:prefix/ids" [date prefix] (events-ids date prefix))
  (GET "/events/archive/:date/:prefix" [date prefix] (events-archive date prefix))
  (GET "/events/:id" [id] (event id))
  (PUT "/events/:id" [id] (update-event id))
  (GET "/heartbeat" [] (heartbeat))
  (GET "/auth-test" [] (auth-test)))

(defn wrap-cors
  "Middleware to add a liberal CORS header."
  [handler]
  (fn [request]
    (let [response (handler request)]
      (when response
        (assoc-in response [:headers "Access-Control-Allow-Origin"] "*")))))

(def app
  ; Delay construction to runtime for secrets config value.
  (delay
    (-> app-routes
       middleware-params/wrap-params
       (jwt/wrap-jwt (:global-jwt-secrets env))
       (middleware-content-type/wrap-content-type)
       (wrap-cors))))

(def schedule-pool (at-at/mk-pool))

(defn run-server []
  (let [port (Integer/parseInt (:bus-port env))]
    (l/info "Start heartbeat")
    (at-at/every
      10000
      #(evidence-log/log!
         {:i "b0003"
          :s "event-bus"
          :c "heartbeat"
          :f "tick"})
      schedule-pool)

    (l/info "Start server on " port)
    (server/run-server @app {:port port})))
