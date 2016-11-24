(ns event-data-event-bus.storage.store
  "A common interface for storing things.
  Satisfied by Redis for component testing and S3 for integration testing and production.")

(defprotocol Store
  "A Store for sotring things."
  (get-string [this k] "Get string key, return string value.")
  (set-string [this k v] "Put string value for string key."))
