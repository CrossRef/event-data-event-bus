(ns event-data-event-bus.archive
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format])
  (:require [event-data-common.storage.store :as store]
            [event-data-common.storage.store :refer [Store]]))



; Prefixes as short as possible to help with S3 load balancing.
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
        ; S3 performs well in parallel, so fetch items in parallel.
        future-event-blobs (map #(future (store/get-string storage %)) event-keys)
        event-blobs (map deref future-event-blobs)
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

(defn storage-path-for-event [event-id]
  (str event-prefix event-id))

(defn save-event
  "Store a serialized event in permanent storage."
  [storage event-id date-str content]

  ; Two keys places: access data of event by event id and set a date-prefixed marker for indexing.
  (let [event-storage-key (storage-path-for-event event-id)
        event-date-storage-key (str day-prefix date-str "/" event-id)]

    ; Store the event by its ID.
    (store/set-string storage event-storage-key content)
    ; Also store the key for the per-day index. We only use the presence of the key. 
    (store/set-string storage event-date-storage-key "")))

