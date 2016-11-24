(ns event-data-event-bus.server
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.server :as server]
            [config.core :refer [env]]
            [compojure.core :refer [defroutes GET POST]]
            [ring.util.response :as ring-response]
            [ring.middleware.params :as middleware-params]
            [ring.middleware.content-type :as middleware-content-type]
            [liberator.core :refer [defresource]]
            [liberator.representation :as representation]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce]
            [clojure.java.io :refer [reader]]
            [event-data-event-bus.storage.redis :as redis]
            [event-data-event-bus.storage.redis :as s3]
            [event-data-event-bus.storage.store :as store])
  (:import [com.auth0.jwt JWTSigner JWTVerifier]
           [java.net URL MalformedURLException InetAddress])
  (:gen-class))

(def event-data-homepage "http://eventdata.crossref.org/")

(def quick-check-expiry
  "How long in milliseconds before mutex for quick-check expires? Should cover any delay in S3 propagagion."
  (*
    1000 ; 1 seconds
    60 ; 1 minute
    60 ; 1 hour
    24 ; 1 day
))

(def up-since
  "Start the clock so we know approximately how long this instance has been running."
  (delay (clj-time/now)))

(def redis-store
  "A redis connection for storing subscription and short-term information."
  (delay (redis/build)))

(def storage
  "A event-data-event-bus.storage.store.Store for permanently storing things"
  (delay
    (condp = (:storage env)
      ; Redis can be used for component testing ONLY.
      "redis" (redis/build)
      "s3" (s3/build)
      ; Default is S3 for production and integration testing.
      (s3/build))))

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
                ; Set a key in redis, then get it, to confirm connection.
                (store/set-string @redis-store "heartbeat-ok" (str now))
                (let [report {:machine_name (.getHostName (InetAddress/getLocalHost))
                              :version (System/getProperty "event-data-event-bus.version")
                              :up-since (str @up-since)
                              :redis-checked (str (store/get-string @redis-store "heartbeat-ok"))
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

(def yyyy-mm-dd-format (clj-time-format/formatter "yyyy-MM-dd"))


(defn timestamp-event
  "Prepare an Event for ingestion. Add timestamp.
   Return [event, 'YYYY-MM-DD']"
  [event]
  (let [now (clj-time/now)
        iso8601 (str now)
        yyyy-mm-dd (clj-time-format/unparse yyyy-mm-dd-format now)]
    [(assoc event :timestamp iso8601) yyyy-mm-dd]))




;   "Create Events."
(defresource events
  []
  :allowed-methods [:post]
  :available-media-types ["application/json"]
  :authorized? (fn
                [ctx]
                ; Authorized if the JWT claims are correctly signed.
                (-> ctx :request :jwt-claims))

  :malformed? (fn [ctx]
                ; Further schema checks will take place here.
                (try
                  [false {::payload (json/read (-> ctx :request :body reader) :key-fn keyword)}]
                  (catch Exception e (do true))))

  :allowed? (fn
              [ctx]
              ; Allowed only if the `sub` of the claim matches the `source_id` of the event.
              (let [; Check that the source ID matches.
                    source-id-ok (= (-> ctx ::payload :source_id)
                                    (get-in ctx [:request :jwt-claims "sub"]))
                    
                    ; Get the event with timestamp.
                    [event yyyy-mm-dd] (timestamp-event (::payload ctx))

                    event-id (:id event)

                    ; Store the event in two places: access by event id and by yyyy-mm-dd prefix
                    event-storage-key (str "events/" event-id)
                    event-date-storage-key (str "day/" yyyy-mm-dd "/" event-id)
                    
                    ; Do a quick check in Redis to see if the event already exists.
                    ; The mutex expires after a bit, long enough to cover any delay in S3 propagation.
                    quick-check-ok (redis/expiring-mutex!? @redis-store (str "exists" event-id) quick-check-expiry)

                    ; Also check permanent storage for existence.
                    storage-check-ok (nil? (store/get-string @storage event-storage-key))

                    allowed? (and
                              ; accept if the source matches
                              source-id-ok

                              ; and we haven't just already sent it
                              quick-check-ok

                              ; and we haven't sent it in the past
                              storage-check-ok)]
                  
                  [allowed? {::event-id event-id
                            ::event-storage-key event-storage-key
                            ::event-date-storage-key event-date-storage-key
                            ::event event}]))

  :post! (fn [ctx]
    (let [json (json/write-str (::event ctx))]
      ; TODO return url
      ; Upload to both places.
      (store/set-string @storage (::event-storage-key ctx) json)
      (store/set-string @storage (::event-date-storage-key ctx) json))))

(defroutes app-routes
  (GET "/" [] (home))
  (POST "/events" [] (events))
  (GET "/heartbeat" [] (heartbeat))
  (GET "/auth-test" [] (auth-test)))

(defn wrap-cors
  "Middleware to add a liberal CORS header."
  [handler]
  (fn [request]
    (let [response (handler request)]
      (when response
        (assoc-in response [:headers "Access-Control-Allow-Origin"] "*")))))

(defn get-token
  "Middleware to retrieve a token from a request. Return nil if missing or malformed."
  [request]
  (let [auth-header (get-in request [:headers "authorization"])]
    (when (and auth-header (.startsWith auth-header "Bearer "))
      (.substring auth-header 7))))

(defn try-verify-token
  "Verify a JWT token using a supplied JWT verifier. Return the claims on success or nil."
  [verifier token]
  (try
    (.verify verifier token)
    ; Can be IllegalStateException, JsonParseException, SignatureException.
    (catch Exception e nil)))

(defn wrap-jwt
  "Return a middleware handler that verifies JWT claims using one of the comma-separated secrets."
  [handler secrets-str]
  (let [secrets (clojure.string/split secrets-str #",")
        verifiers (map #(new JWTVerifier %) secrets)]
    (fn [request]
      (let [token (get-token request)
            matched-token (first (keep #(try-verify-token % token) verifiers))]
        (if matched-token
          (handler (assoc request :jwt-claims matched-token))
          (handler request))))))

(def app
  ; Delay construction to runtime for secrets config value.
  (delay
    (-> app-routes
       middleware-params/wrap-params
       (wrap-jwt (:jwt-secrets env))
       (middleware-content-type/wrap-content-type)
       (wrap-cors))))

(defn run-server []
  (let [port (Integer/parseInt (:port env))]
    (l/info "Start server on " port)
    (server/run-server @app {:port port})))
