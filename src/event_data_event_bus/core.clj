(ns event-data-event-bus.core
  (:require [event-data-event-bus.server :refer [run-server]])
  (:require [event-data-event-bus.archive :refer [run-archive-schedule run-archive-all-since]])
  (:gen-class))

(defn -main
  [& args]
  (condp = (first args) 
    "schedule" (run-archive-schedule)
    "historical-archive" (run-archive-all-since (second args))
    (run-server)))



