(ns event-data-event-bus.archive
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l]
            [clj-time.core :as clj-time])
  (:require [event-data-event-bus.storage.store :as store]
            [event-data-event-bus.storage.store :refer [Store]]))

(def day-prefix
  "Prefix under which events are stored with their date, e.g. 'd/2016-11-27/86accb20-1c8f-483d-8567-52ad031ba190'"
  "d/")

(def event-prefix
  "Prefix under which events are stored for retrieval, e.g. 'e/f0ca191e-b8af-485c-b06b-fbee4cf0423b'"
  "e/")

(def archive-prefix
  "Prefix under which per-day archives are stored."
  "a/")

(defn archive-for
  "Generate archive for given YYYY-MM-DD date string prefix."
  [storage date-str]
  (let [; All events we're looking for will have been stored with the `day-prefix` and the YYYY-MM-DD prefix.
        date-keys (store/keys-matching-prefix storage (str day-prefix date-str))

        ; We get back a key with the day-prefix. The actual data is stored in the event-prefix.
        ; i.e. "d/YYYY-MM-DD/1234" -> "e/1234"
        prefix-length (+ (.length day-prefix) (.length "YYYY-MM-DD/"))
        event-keys (map #(str event-prefix (.substring ^String % prefix-length)) date-keys)

        ; Retrieve every event for every key.
        event-blobs (map (partial store/get-string storage) event-keys)
        all-events (map json/read-str event-blobs)

        timestamp (str (clj-time/now))]
    (l/info "Archive for" date-str "got" (count event-keys))
    {"archive-generated" timestamp
     "events" all-events}))

(defn save-archive
  "Save or update archive in storage."
  [storage date-str]
  (let [archive (archive-for storage date-str)]
    (store/set-string storage (str archive-prefix date-str) (json/write-str archive))))