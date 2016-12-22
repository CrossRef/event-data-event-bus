(ns event-data-event-bus.downstream
  "Send Events on their way downstream.
  Two kinds of broadcast, 'live' and 'batch'.
  Configuration entries are configured as environment variables of the following pattern:
  BROADCAST-*-JWT
  BROADCAST-*-ENDPOINT
  BROADCAST-*-NAME
  BROADCAST-*-TYPE

  The * wildcard stands for a label for that broadcast subscriber.
  If the configuration isn't valid, an exception will be thrown which should prevent the application from starting."

  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clojure.string :as string]
            [config.core :refer [env]]
            [event-data-common.backoff :as backoff]
            [org.httpkit.client :as client]))

(def retries
  "Try to deliver this many times downstream."
  5)

(def retry-delay
  "Wait in milliseconds before first redelivery attempt. Retry will double each time."
  (* 10 1000))

(defn parse-broadcast-config
  "Read a broadcast structure from a structure containing environment variables.
   Return {:live list-of-endpoints
           :batch list-of-endpoints}
   Endpoints are specified as {:label, :jwt, :endpoint, :name, :type}
   Return nil on error."
   [environment]
   (let [; all keys as strings (they're supplied as keywords).
         all-keys (map name (keys environment))
         ; with-prefix (filter #(string/starts-with? % "broadcast-") all-keys)
         ; as triples of ["broadcast" «label» «key»]
         splitten (map #(string/split % #"-") all-keys)
         ; Those with the correct suffix.
         with-suffix (filter #(and (= (count %) 3)
                                    (= (first %) "broadcast")
                                    (#{"jwt" "endpoint" "name" "type"} (nth % 2)))
                              splitten)

         ; Into groups by the label.
         ; A seq of vectors of keys per label, e.g.
         ; ([["broadcast" "3" "name"] ["broadcast" "3" "jwt"] ["broadcast" "3" "endpoint"] ["broadcast" "3" "type"]]
         ;  [["broadcast" "1" "name"] ["broadcast" "1" "endpoint"] ["broadcast" "1" "jwt"] ["broadcast" "1" "type"]]
         ; ... )
         per-label-groups (vals (group-by second with-suffix))

         ; As a sequence of complete config per label.
         ; A seq of {:jwt :endpoint :name :type :label}
         downstreams (map (fn [per-label-group]
                            ; Start with a label, which is the second component (the wildcard).
                            (into {:label (second (first per-label-group))}
                             ; Assoc in all of the keys in the group.
                             (map (fn [item]
                               [; The key is the third item of the variable name, e.g. 'jwt'
                                (keyword (nth item 2))
                                ; To get the value, we recombine the triple into a string, then a keyword to look up in the environment.
                                (get environment (keyword (string/join "-" item)))])
                             per-label-group)))
                           per-label-groups)

         all-keys-present? (every? #(= (-> % keys set) #{:jwt :endpoint :name :type :label}) downstreams)
         all-types-known? (every? #{"live" "batch"} (map :type downstreams))
    
         ; Then we just want to group by the type (and turn the key into a keyword).
         ; Also return the list as a set because order isn't significant.
         type-groups (into {} (map (fn [[k v]] [(keyword k) (set v)])
                                     (group-by :type downstreams)))

         ; Default values for each, in case there are none of either.
         result (merge {:live #{} :batch #{}} type-groups)]

    (when-not all-keys-present?
      (log/error "Failed to read config for downstream subscribers: Missing config keys."))

    (when-not all-types-known?
      (log/error "Failed to read config for downstream subscribers: Unknown subscription type."))

   ; Nil if it didn't work.
   (when (and all-keys-present? all-types-known?) result)))

(defn load-broadcast-config
  []
  "Load broadcast configuration structure from environment variable config."
   (let [value (parse-broadcast-config env)]
      (log/info "Loaded broadcast config:" (count (:live value)) "live and " (count (:batch value)) "batch")
   value))
   
(def downstream-config-cache (delay (load-broadcast-config)))

(defn broadcast-live
  "Accept incoming event and broadcast to live downstream subscribers."
  [event-structure]
  ; Most of the heavy lifting is done by `backoff`.
  (let [live-downstreams (:live (load-broadcast-config))
        body-json (json/write-str event-structure)]
    (log/info "Recieve event for live broadcast:" (:id event-structure) "to" (count live-downstreams))
    (doseq [downstream live-downstreams]
      (backoff/try-backoff
        ; Exception thrown if not 200 or 201, also if some other exception is thrown during the client posting.
        #(let [response @(client/post (:endpoint downstream) {:body body-json})]
            (when-not (#{201 200} (:status response)) (throw (new Exception (str "Failed to send to downstream " (:label downstream) "at URL: " (:endpoint downstream) " with status code: " (:status response))))))
        retry-delay
        retries
        ; Only log info on retry because it'll be tried again.
        #(log/info "Error broadcasting" (:id event-structure) "to downstream" (:label downstream) "with exception" (.getMessage %))
        ; But if terminate is called, that's a serious problem.
        #(log/error "Failed to send event" (:id event-structure) "to downstream" (:label downstream))
        #(log/info "Finished broadcasting" (:id event-structure) "to downstream" (:label downstream))))))
