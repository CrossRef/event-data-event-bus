(ns event-data-event-bus.storage.s3
  "Storage interface for AWS S3."
  (:require [config.core :refer [env]]
            [event-data-event-bus.storage.store :refer [Store]]
            [clojure.tools.logging :as l])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest ObjectMetadata S3Object ObjectListing S3ObjectSummary]
           [com.amazonaws AmazonServiceException AmazonClientException]
           [com.amazonaws.regions Regions Region]
           [org.apache.commons.io IOUtils]
           [java.io InputStream]))

; Default retry policy of 3 to cope with failure.
; http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html#DEFAULT_RETRY_POLICY
(defn get-aws-client
  []
  (let [client (new AmazonS3Client (new BasicAWSCredentials (:s3-key env) (:s3-secret env)))]
    (.setRegion client (Region/getRegion (Regions/fromName (:s3-region-name env))))
    client))

(defn upload-string
  "Upload a stream. Exception on failure."
  [client bucket-name k v content-type]
  (let [bytes (.getBytes v)
        metadata (new ObjectMetadata)
        _ (.setContentType metadata content-type)
        _ (.setContentLength metadata (alength bytes))
        request (new PutObjectRequest bucket-name k (new java.io.ByteArrayInputStream bytes) metadata)]
    (.putObject client request)))

(defn download-string
  "Download a stream. Exception on failure."
  [client bucket-name k]
  (when (.doesObjectExist client bucket-name k)
    (let [^S3Object object (.getObject client bucket-name k)
          ^InputStream input (.getObjectContent object)
          result (IOUtils/toString input "UTF-8")]
      (.close input)
      result)))

(defn get-keys
  "Return a seq of String keys for an ObjectListing"
  [^ObjectListing listing]
  (map #(.getKey ^S3ObjectSummary %) (.getObjectSummaries listing)))

(defn list-objects
  "List the next page of objects, or the first page if prev-listing is nil."
  [client bucket-name prefix prev-listing]
  (if-not prev-listing
    ; Start at first page.
    (let [^ObjectListing this-listing (.listObjects client bucket-name prefix)]
      (if-not (.isTruncated this-listing)
        ; This is the last page
        (get-keys this-listing)

        ; This is not the last page.
        ; Recurse with the listing as the next-page token.
        (lazy-cat (get-keys this-listing) (list-objects client bucket-name prefix this-listing))))

    ; Start at subsequent page.
    (let [^ObjectListing this-listing (.listNextBatchOfObjects client prev-listing)]
      (if-not (.isTruncated this-listing)
        ; This is the last page
        (get-keys this-listing)

        ; This is not the last page.
        ; Recurse with the listing as the next-page token.
        (lazy-cat (get-keys this-listing) (list-objects client bucket-name prefix this-listing))))))


(defrecord S3Connection
  [^AmazonS3Client client]
  
  Store
  (get-string [this k]
    (l/debug "Store get" k)
    (download-string client (:s3-bucket-name env) k))

  (set-string [this k v]
    (l/debug "Store set" k)
    (upload-string client (:s3-bucket-name env) k v "application/json"))

  (keys-matching-prefix [this prefix]
    (l/debug "Store scan prefix" prefix)
    (list-objects client (:s3-bucket-name env) prefix nil)))

(defn build
  "Build a S3Connection object."
  []
  (let [client (get-aws-client)]
    (S3Connection. client)))

