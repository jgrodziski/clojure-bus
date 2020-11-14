(ns clojure-bus.kafka.kafka-manager
  (:require
   [jackdaw.admin :as ja]
   [jackdaw.client :as jc]
   [jackdaw.client.log :as jcl]
   [jackdaw.serdes.edn :as jse]

   [clojure-bus.kafka.kafka :as k]
   [clojure-bus.utils.log :as log]
   )
  (:import
   (org.apache.kafka.clients.consumer Consumer)
   (org.apache.kafka.common.serialization Serdes StringDeserializer)))


;;; -------------------
;;; TOPICS
;;; -------------------
(defn kafka-admin-client-config
  ([]
   (kafka-admin-client-config "localhost:9092"))
  ([bootstrap-servers]
   {"bootstrap.servers" bootstrap-servers}))

(defn topic-config
  "Takes a topic name and (optionally) a key and value serde and
  returns a topic configuration map, which may be used to create a
  topic or produce/consume records."
  ([topic-name]
   (topic-config topic-name (Serdes/String) (Serdes/String)))

  ([topic-name key-serde value-serde]
   (topic-config topic-name 2 key-serde value-serde))

  ([topic-name parition-count key-serde value-serde]
   {:topic-name              topic-name
    :partition-count    parition-count
    :replication-factor 1
    :key-serde          key-serde
    :value-serde        value-serde}))

(defn list-topics
  "List all topics on cluster"
  ([]
   (list-topics (kafka-admin-client-config)))
  ([admin-client-config]
   (with-open [client (ja/->AdminClient admin-client-config)]
     (ja/list-topics client))))

(defn describe-topics
  "List all topics on cluster"
 fk ([topics]
   (describe-topics topics (kafka-admin-client-config)))
  ([topics admin-client-config]
   (with-open [client (ja/->AdminClient admin-client-config)]
     (ja/describe-topics client topics))))

(defn create-topics
  "Takes a list of topics and creates these using the names given."
  ([topic-config-list]
   (create-topics topic-config-list (kafka-admin-client-config)))
  ([topic-config-list admin-client-config]
   (with-open [client (ja/->AdminClient admin-client-config)]
     (ja/create-topics! client topic-config-list))))

(defn delete-topics
  "Takes a list of topic and deletes any Kafka
  topics that match."
  ([topics]
   (delete-topics topics (kafka-admin-client-config)))
  ([topics admin-client-config]
   (with-open [client (ja/->AdminClient admin-client-config)]
     (ja/delete-topics! client (map (fn [topic] {:topic-name topic}) topics)))))

;;; -------------------
;;; CONSUME MESSAGES
;;; -------------------

(defn kafka-consumer-config
  [bootstrap-servers group-id]
  {"bootstrap.servers"  bootstrap-servers
   "group.id"           group-id
   "auto.offset.reset"  "earliest"
   "enable.auto.commit" "false"
   "key.deserializer"   (.getName (.getClass (StringDeserializer.)))
   "value.deserializer" (.getName (.getClass (StringDeserializer.)))})

(defn get-records
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of maps."
  ([consumer-config topic-config]
   (let [consumer-config consumer-config]
     (with-open [^Consumer consumer (-> (jc/subscribed-consumer consumer-config [topic-config])
                                        (jc/seek-to-beginning-eager))]
       (doall (seq (jcl/log-until-inactivity consumer 200)))))))

(defn topics-exist-or-create
  "Checks if the passed topics exist, if not, create them"
  [admin-client-config topics-config]
  (let [missing-topics (->> (list-topics admin-client-config)
                            (map :topic)
                            set
                            (clojure.set/difference (set (map :topic topics-config))))]
    (when (and missing-topics (seq missing-topics))
      (log/info {:service ::topics.exist-or-create :missing-topics missing-topics :admin-client-config admin-client-config} "Create missing kafka topics" )
      (create-topics (map k/topic-config missing-topics) admin-client-config))))

(comment
  (list-topics)

  (describe-topics [{:topic-name "abonnement"}])

  (delete-topics ["abonnement"])

  (create-topics (map topic-config ["abonnement" "account" "diffusion"]))

  (get-records (kafka-consumer-config (bootstrap-servers) "abonnement_groupid") (topic-config "abonnement"))

  )
