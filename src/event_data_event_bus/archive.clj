(ns event-data-event-bus.archive
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.periodic :as clj-time-periodic]
            [event-data-common.storage.store :as store]
            [event-data-common.storage.store :refer [Store]]
            [event-data-common.storage.s3 :as s3]
            [event-data-common.date :as date]
            [config.core :refer [env]]
            [clojurewerkz.quartzite.triggers :as qt]
            [clojurewerkz.quartzite.jobs :as qj]
            [clojurewerkz.quartzite.schedule.daily-interval :as daily]
            [clojurewerkz.quartzite.schedule.calendar-interval :as cal]
            [clojurewerkz.quartzite.jobs :refer [defjob]]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.schedule.cron :as qc]
            [clojure.math.combinatorics :as combinatorics]))

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

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

; Standard length is 2 for pre-emptive generation of archive, but more can be requested.
(def standard-prefix-length 2)
(def hexadecimal [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \a \b \c \d \e \f])

(defn event-prefixes-length
  [length]
  (map #(apply str %) (combinatorics/selections hexadecimal length)))

(defn event-ids-for
  "Get a sequence of Event IDs for the given date and prefix."
  [storage date-str event-id-prefix]
  (let [; We are filtering the day index by the prefix which includes part of the event ID.
        ; We will then remove the date part of the prefix to find the event IDs.
        full-prefix (str day-prefix date-str "/" event-id-prefix)

        ; All events we're looking for will have been stored with the `day-prefix` and the YYYY-MM-DD prefix.
        date-keys (store/keys-matching-prefix storage full-prefix)

        ; We get back a key with the day-prefix. The actual data is stored in the event-prefix.
        ; i.e. "d/YYYY-MM-DD/1234" -> "e/1234"
        prefix-length (+ (.length day-prefix) (.length "YYYY-MM-DD/"))
        event-ids (map #(.substring ^String % prefix-length) date-keys)]
    
    event-ids))

(defn archive-for
  "Generate archive for given YYYY-MM-DD date string prefix plus the event ID prefix."
  [storage date-str event-id-prefix]
  (let [event-ids (event-ids-for storage date-str event-id-prefix)
        num-keys (count event-ids)

        ; Retrieve every event for every key.
        ; S3 performs well in parallel, so fetch items in parallel.
        counter (atom 0)
        future-event-blobs (map #(future
                                   (swap! counter inc)
                                   (when (zero? (mod @counter 1000))
                                     (log/info "Building archive for" date-str "retrieved" @counter "/" num-keys " = " (int (* 100 (/ @counter num-keys))) "%"))
                                   (store/get-string storage (str event-prefix %))) event-ids)
        event-blobs (map deref future-event-blobs)
        all-events (map json/read-str event-blobs)

        timestamp (clj-time-format/unparse date-format (clj-time/now))]
    (log/info "Archive for" date-str "got" num-keys "keys and" (count event-blobs) "events")
    {"archive-generated" timestamp
     "date" date-str
     "prefix" event-id-prefix
     "events" all-events}))

(defn get-or-generate-archive
  "Generate the archive or retrieve and save it."
  [storage date-str event-id-prefix]
  (log/info "Get or generate archive for" date-str "/" event-id-prefix)
  (let [storage-key (str archive-prefix date-str "/" event-id-prefix)
        ; Get the ready made archive file...
        retrieved (store/get-string storage storage-key)
        ; Or re-generate it.
        generated (when-not retrieved (archive-for storage date-str event-id-prefix))]

    ; If we had to generate it, save for next time.
    (when generated
      (store/set-string storage (str archive-prefix date-str "/" event-id-prefix) (json/write-str generated)))

    (if retrieved
      (log/info "Retrieved pre-built archive for" date-str "/" event-id-prefix "from" storage-key)
      (log/info "Generated archive for" date-str "/" event-id-prefix "at" storage-key))

    (or retrieved generated)))

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

(defn invalidate-archive-for-event-id
  "Invalidate the cached archive(s) that contain the given Event ID."
  [storage event-id date-str]
    (let [storage-key (str archive-prefix date-str "/" (.substring event-id 0 standard-prefix-length))]
      (log/info "Invalidate archive with key" storage-key)
      (store/delete storage storage-key)))

(def storage
  (delay (s3/build
         (:bus-s3-key env)
         (:bus-s3-secret env)
         (:bus-s3-region-name env)
         (:bus-s3-bucket-name env))))

(defjob yesterday-archive-job
  [ctx]
  (log/info "Starting yesterday's archive...")
  (doseq [event-id-prefix (event-prefixes-length standard-prefix-length)]
    (log/info "Saving yesterday's archive for prefix" event-id-prefix)
    (get-or-generate-archive
      @storage
      (clj-time-format/unparse date/yyyy-mm-dd-format (clj-time/minus (clj-time/now) (clj-time/days 1)))
      event-id-prefix))
    (log/info "Saved yesterday's archive."))

(defn run-archive-schedule
  "Start schedule to generate daily archive. Block."
  []
  (log/info "Start scheduler")
  (let [s (-> (qs/initialize) qs/start)
        job (qj/build
              (qj/of-type yesterday-archive-job)
              (qj/with-identity (qj/key "jobs.noop.1")))
        trigger (qt/build
                  (qt/with-identity (qt/key "triggers.1"))
                  (qt/start-now)
                  (qt/with-schedule (qc/cron-schedule "0 30 0 * * ?")))]
  (qs/schedule s job trigger)))

(defn run-archive-all-since
  "Accept YYYY-MM-DD date string and make sure all archives exist between then and yesterday."
  [date-str]
  (let [start-date (clj-time-format/parse date/yyyy-mm-dd-format date-str)
        end-date (clj-time/minus (clj-time/now) (clj-time/days 1))
        date-range (take-while #(clj-time/before? % end-date)
                     (clj-time-periodic/periodic-seq start-date (clj-time/days 1)))]
    (doseq [date date-range]
      (doseq [event-id-prefix (event-prefixes-length standard-prefix-length)]
        (log/info "Check" date "prefix" event-id-prefix)
          (get-or-generate-archive @storage (clj-time-format/unparse date/yyyy-mm-dd-format date) event-id-prefix)))
    (log/info "Done checking dates")))
