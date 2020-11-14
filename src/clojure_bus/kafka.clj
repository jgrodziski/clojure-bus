(ns clojure-bus.kafka
  (:require [clojure.java.io :as io :refer [input-stream]]
            [jsonista.core :as json]
            [byte-streams :refer [bytes=]]
            [jackdaw.client :as jc]
            [jackdaw.serdes.json :as jsj]
            [jackdaw.streams :as js]

            [clojure-bus.log :refer [info debug]]
            [clojure-bus.json]                     ;require the cheshire json encoder for java.time
            [clojure-bus.transit :as transit])
  (:import (java.nio.charset StandardCharsets)
           (org.apache.kafka.common.serialization Serdes)
           (org.apache.kafka.common.utils Bytes)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                        ;;
;;                    Config Stuff                        ;;
;;                                                        ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn producer-config
  [bootstrap-servers]
  {"bootstrap.servers" bootstrap-servers
   "max.block.ms" 30000};;block for 30s (default to 60s)
  )

(defn topic-config
  "Takes a topic name and (optionally) a key and value serde and
  returns a topic configuration map, which may be used to create a
  topic or produce/consume records."
  ([topic-name]
   (topic-config topic-name 1))
  ([topic-name partition-count]
   (topic-config topic-name partition-count (Serdes/ByteArray) (Serdes/ByteArray)))
  ([topic-name partition-count key-serde value-serde]
   {:topic-name         topic-name
    :partition-count    partition-count
    :replication-factor 1
    :key-serde          key-serde
    :value-serde        value-serde}))

(defn consumer-config
  ([application-id bootstrap-servers]
   (let [serde (.getName (.getClass (.deserializer (Serdes/ByteArray))))]
     (consumer-config application-id bootstrap-servers serde serde)))
  ([application-id bootstrap-servers key-serde value-serde]
   {"application.id"            application-id
    "bootstrap.servers"         bootstrap-servers
    "cache.max.bytes.buffering" "0"
    "key.deserializer"          key-serde
    "value.deserializer"        value-serde
    "enable.auto.commit"        "false"
    "auto.offset.reset"         "earliest"}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                        ;;
;;          Serializer/Deserializer (Serde) functions     ;;
;;                                                        ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn data->transit-json-bytes
  "Serialize data structure to json byte array"
  [x]
  (.getBytes (transit/encode x)))

(defn transit-json-bytes->data
  "Deserialize json byte array tp data structure"
  [b]
  (transit/decode (String. b)))

(defn str->bytes
  "Serialize data structure to json byte array"
  [^String s]
  (when s
    (.getBytes s StandardCharsets/UTF_8)))

(defn bytes->str
  "Deserialize byte array to string"
  [^Bytes b]
  (when b
    (if (instance? java.lang.String b)
      b
      (String. b StandardCharsets/UTF_8))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                   ;;
;;          producer functions                       ;;
;;                                                   ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn publish!
  "Takes a topic config and record value, a key and produces to a Kafka topic. Returns a future with datafield record metadata"
  [producer-config topic-config key value]
  (with-open [client (jc/producer producer-config topic-config)]
    (jc/produce! client
                 topic-config
                 (str->bytes key)
                 (data->transit-json-bytes value))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                   ;;
;;          Consumer stream functions                ;;
;;                                                   ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn topology-for-each
  "Configures the given `builder` to stream the specified `topics`. `f` is function of one argument which is a map of two entries {:key k :value v}"
  [builder topics-config f]
  (-> builder
      (js/kstreams topics-config)
      (js/for-each! (fn [[k v]]
                      (f {:key (bytes->str k)
                          :value (transit-json-bytes->data v)}))))
  builder)

(defn topology-filter
  "Configures the given `builder` to stream the specified `topics` while filtering with key `k` then applying function `f`, a function of one argument which is a map of two entries {:key k :value v}"
  [builder topics-config k f]
  (info {:service :topology-filter :config :topics-config :k k} "Build kafka topology filter")
  (-> builder
      (js/kstreams topics-config)
      (js/filter (fn [[k2 v]] (bytes= k k2)))
      (js/for-each! (fn [[k2 v]]
                      (f {:key (bytes->str k2)
                          :value (transit-json-bytes->data v)}))))
  builder)

(defn filter-topology-builder [k f]
  (fn [topics-config]
    (fn [builder]
      (-> (js/kstreams builder topics-config )
          (js/filter (fn [[k2 v]] (= (bytes->str k)
                                     (transit-json-bytes->data k2))))
          (js/for-each! (fn [[k2 v]]
                          (f {:key (bytes->str k2)
                              :value (transit-json-bytes->data v)}))))
      builder)))

(defn start-event-streams
  ([consumer-config topics-config topology-fn callback-fn]
   (let [event-streams (-> (js/streams-builder)
                           (topology-for-each topics-config callback-fn)
                           (js/kafka-streams consumer-config))]
     (js/start event-streams)
     event-streams))
  ([consumer-config topics-config topology-fn k f]
   (let [event-streams (-> (js/streams-builder)
                           (topology-filter topics-config k f)
                           (js/kafka-streams consumer-config))]
     (js/start event-streams)
     event-streams)))
