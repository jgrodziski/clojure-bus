(ns clojure-bus.utils.json
  (:require [cheshire.generate :as generate]
            [jsonista.core :as jsonista]

            [clojure-bus.utils.uuid :as uuid]
            )
  (:import [java.time Instant LocalDate LocalDateTime ZonedDateTime ZoneId]
           [com.fasterxml.jackson.core JsonGenerator]))

;; Jackson encoders
(defn to-string-encoder [data ^JsonGenerator jsonGenerator]
  (.writeString jsonGenerator (.toString data)))

;; Cheshire link to jackson encoders
(generate/add-encoder Instant to-string-encoder)
(generate/add-encoder LocalDate to-string-encoder)
(generate/add-encoder LocalDateTime to-string-encoder)
(generate/add-encoder ZonedDateTime to-string-encoder)

;; Jackson mapper to use with jsonista
(def mapper (jsonista/object-mapper {:decode-key-fn true
                                     :encoders {Instant to-string-encoder
                                                LocalDate to-string-encoder
                                                LocalDateTime to-string-encoder
                                                ZonedDateTime to-string-encoder}}))

(def suffix->type-fn {"-at"   (fn [s] (java.time.Instant/parse s))
                      "-id"   (fn [s] (uuid/of s))
                      "-ref"  (fn [s] (uuid/of s))
                      "-type" (fn [s] (keyword s))})

(def suffixes (set (keys suffix->type-fn)))

(defn suffix [k]
  (let [s (name k)
        l (count s)
        dash-offset (clojure.string/last-index-of s "-")]
    (when dash-offset
      (subs s dash-offset l))))

(defn suffix-match? [k]
  (suffixes (suffix k)))

(defn typify [k s]
  (try
    (if-let [f (get suffix->type-fn (suffix k))]
      (f s)
      s)
    (catch java.lang.IllegalArgumentException iae
      s)))

(defn typify-string
  "Recursively transform all map values that end with specific suffix to the typed object (Instant, UUID, etc.)"
  [m]
  (clojure.walk/postwalk (fn [x]
                           (if (vector? x)
                             (let [[k v] x]
                               (if (and (suffix-match? k) (instance? java.lang.String v))
                                 [k (typify k v)]
                                 [k v]))
                             x)) m))

