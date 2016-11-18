(ns event-data-event-bus.server
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.server :as server]
            [config.core :refer [env]]
            [compojure.core :refer [defroutes GET]]
            [ring.util.response :as ring-response]
            [ring.middleware.params :as middleware-params]
            [ring.middleware.content-type :as middleware-content-type]
            [liberator.core :refer [defresource]]
            [liberator.representation :as representation]
            [clj-time.core :as clj-time])
  (:import [com.auth0.jwt JWTSigner JWTVerifier]
           [java.net URL MalformedURLException InetAddress])
  (:gen-class))

(def event-data-homepage "http://eventdata.crossref.org/")
(def up-since (atom nil))

(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
                (representation/ring-response
                  (ring-response/redirect event-data-homepage))))

(defresource heartbeat
  []
  :allowed-methods [:get]
  :available-media-types ["application/json"]
  :handle-ok (fn [context]
              (let [report {:machine_name (.getHostName (InetAddress/getLocalHost))
                            :version (System/getProperty "event-data-event-bus.version")
                            :up-since (str @up-since)
                            :status "OK"}]
                report)))

(defresource auth-test
  []
  :allowed-methods [:get]
  :available-media-types ["application/json"]
  :authorized? (fn [ctx]
                (:jwt-claims ctx))
  :handle-ok {"status" "ok"})

(defroutes app-routes
  (GET "/" [] (home))
  (GET "/heartbeat" [] (heartbeat))
  (GET "/auth-test" [] (auth-test)))

(defn wrap-cors [handler]
  (fn [request]
    (let [response (handler request)]
      (assoc-in response [:headers "Access-Control-Allow-Origin"] "*"))))

(defn get-token
  "Retrieve a token from a request. Return nil if missing or malformed."
  [request]
  (let [auth-header (get-in request [:headers "authorization"])]
    (when (and auth-header (.startsWith auth-header "Bearer "))
      (.substring auth-header 7))))

(defn try-verify-token
  "Verify a JWT token using a "
  [verifier token]
  (try
    (.verify verifier token)
    ; Can be IllegalStateException, JsonParseException, SignatureException.
    (catch Exception e (prn "EX" verifier e))))

(defn wrap-jwt [handler secrets-str]
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
    (reset! up-since (clj-time/now))
    (l/info "Start server on " port)
    (server/run-server @app {:port port})))
