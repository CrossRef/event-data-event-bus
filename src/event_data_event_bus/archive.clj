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
            [clojurewerkz.quartzite.schedule.cron :as qc]))

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
        event-keys (doall (map #(str event-prefix (.substring ^String % prefix-length)) date-keys))
        num-keys (count event-keys)

        ; Retrieve every event for every key.
        ; S3 performs well in parallel, so fetch items in parallel.
        counter (atom 0)
        future-event-blobs (map #(future
                                   (swap! counter inc)
                                   (when (zero? (mod @counter 1000))
                                     (log/info "Building archive for" date-str "retrieved" @counter "/" num-keys " = " (int (* 100 (/ @counter num-keys))) "%"))
                                   (store/get-string storage %)) event-keys)
        event-blobs (map deref future-event-blobs)
        all-events (map json/read-str event-blobs)

        timestamp (str (clj-time/now))]
    (log/info "Archive for" date-str "got" num-keys "keys and" (count event-blobs) "events")
    {"archive-generated" timestamp
     "events" all-events}))

(defn save-archive
  "Save or update archive in storage."
  [storage date-str]
  (let [archive (archive-for storage date-str)]
    (store/set-string storage (str archive-prefix date-str) (json/write-str archive))))

(defn save-archive-if-not-exists
  [storage date]
  (let [date-str (clj-time-format/unparse date/yyyy-mm-dd-format date)
        filename (str archive-prefix date-str)
        exists? (= (store/keys-matching-prefix storage filename) [filename])]
    (log/info "Build archive, if missing, for" filename)
    (if exists?
      (log/info "Exists, skipping" filename)
      (save-archive storage date-str)))
  (log/info "Done archive"))

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

(def storage
  (delay (s3/build (:s3-key env) (:s3-secret env) (:s3-region-name env) (:s3-bucket-name env))))

(defjob yesterday-archive-job
  [ctx]
  (log/info "Starting yesterday's archive...")
  
  (save-archive-if-not-exists
    @storage
    (clj-time/minus (clj-time/now) (clj-time/days 1)))

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
                  (qt/with-schedule (daily/schedule
                                      (qc/cron-schedule "30 0 * * *"))))]
  (qs/schedule s job trigger)))

(defn run-archive-all-since
  "Accept YYYY-MM-DD date string and make sure all archives exist between then and yesterday."
  [date-str]
  (let [start-date (clj-time-format/parse date/yyyy-mm-dd-format date-str)
        end-date (clj-time/minus (clj-time/now) (clj-time/days 1))
        date-range (take-while #(clj-time/before? % end-date)
                     (clj-time-periodic/periodic-seq start-date (clj-time/days 1)))]
    (doseq [date date-range]
      (log/info "Check" date)
      (save-archive-if-not-exists @storage date))
    (log/info "Done checking dates")))
