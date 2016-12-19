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
            [config.core :refer [env]]))

(def downstream-config-cache (atom nil))

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
                                     (group-by :type downstreams)))]


    (when-not all-keys-present?
      (log/error "Failed to read config for downstream subscribers: Missing config keys."))

    (when-not all-types-known?
      (log/error "Failed to read config for downstream subscribers: Unknown subscription type."))

   ; Nil if it didn't work.
   (when (and all-keys-present? all-types-known?) type-groups)))

(defn load-broadcast-config
  []
  "Load broadcast configuration structure from environment variable config.
   Then cache in an atom."
  (if-let [cached @downstream-config-cache]
    cached
    (reset! downstream-config-cache (parse-broadcast-config env))))
