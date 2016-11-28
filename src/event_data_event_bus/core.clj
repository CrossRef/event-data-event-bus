(ns event-data-event-bus.core
  (:require [event-data-event-bus.server :refer [run-server]])
  (:gen-class))

(defn -main
  [& args]
  (run-server))
