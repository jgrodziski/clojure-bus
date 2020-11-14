(ns kafka-bus.transit
  "these transit write and read handler deals with common types we found in transit data, notably dates types"
  (:require [cognitect.transit :as transit])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
           [java.time Instant LocalDate LocalDateTime ZonedDateTime ZoneId]))

(def java-time-instant-writer
  (transit/write-handler
   (constantly "jti");this is the tag that denotes java.time.Instant type
   (fn [v] [(.getEpochSecond v) (.getNano v)]);representation
   (fn [v] (.toString v));string representation
   ))

(def java-time-local-date-writer
  (transit/write-handler
   (constantly "jtld");this is the tag that denotes java.time LocalDate type
   (fn [v] [(.getYear v)
           (.getMonthValue v)
           (.getDayOfMonth v)]);representation
   (fn [v] (.toString v));string representation
   ))

(def java-time-local-date-time-writer
  (transit/write-handler
   (constantly "jtldt");this is the tag that denotes java.time.LocalDateTime
   (fn [v] [(.getYear v)
           (.getMonthValue v)
           (.getDayOfMonth v)
           (.getHour v)
           (.getMinute v)
           (.getSecond v)
           (.getNano v)]);representation
   (fn [v] (.toString v));string representation
   ))

(def java-time-zoned-date-time-writer
  (transit/write-handler
   (constantly "jtzdt");this is the tag that denotes java.time.ZonedDateTime
   (fn [v] [(.getYear v)
           (.getMonthValue v)
           (.getDayOfMonth v)
           (.getHour v)
           (.getMinute v)
           (.getSecond v)
           (.getNano v)
           (.toString (.getOffset v))
           (.toString (.getZone v))]);representation
   (fn [v] (.toString v));string representation
   ))


(def java-time-instant-reader
  (transit/read-handler
   (fn [[epoch-s nano-adj]] (java.time.Instant/ofEpochSecond epoch-s nano-adj))))

(def java-time-local-date-reader
  (transit/read-handler
   (fn [[year month day]] (LocalDate/of year month day))))

(def java-time-local-date-time-reader
  (transit/read-handler
   (fn [[y m d h mn s n]]
     (LocalDateTime/of y m d h mn s n))))

(def java-time-zoned-date-time-reader
  (transit/read-handler
   (fn [[y m d h mn s n o z]]
     (ZonedDateTime/of y m d h mn s n (ZoneId/of z)))))


(def custom-type->transit-writer {java.time.Instant java-time-instant-writer
                                  java.time.LocalDate java-time-local-date-writer
                                  java.time.LocalDateTime java-time-local-date-time-writer
                                  java.time.ZonedDateTime java-time-zoned-date-time-writer})

(def custom-tag->transit-reader {"jti" java-time-instant-reader
                                 "jtld" java-time-local-date-reader
                                 "jtldt" java-time-local-date-time-reader
                                 "jtzdt" java-time-zoned-date-time-reader})

(def java-time-read-handler-map (transit/read-handler-map custom-tag->transit-reader))
(def java-time-write-handler-map (transit/write-handler-map custom-type->transit-writer))

(defn encode
  ([x] (encode x :json))
  ([x writer-type] (encode x writer-type {}))
  ([x writer-type types] (encode x writer-type types nil))
  ([x writer-type types options]
   (let [out (ByteArrayOutputStream. 2048)
         writer (transit/writer out writer-type (merge {:handlers (transit/write-handler-map (concat custom-type->transit-writer types))} options))]
     (transit/write writer x)
     (.toString out))))

(defn decode
  ([x] (decode x :json))
  ([x reader-type] (decode x reader-type {}))
  ([x reader-type types] (decode x reader-type types nil))
  ([x reader-type types options]
   (-> x
       (.getBytes)
       (ByteArrayInputStream.)
       (transit/reader reader-type (merge {:handlers (transit/read-handler-map (concat custom-tag->transit-reader types))} options))
       (transit/read))))
