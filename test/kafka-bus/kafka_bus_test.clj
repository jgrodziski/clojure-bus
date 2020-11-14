(ns kafka-bus.kafka-bus-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test :refer [deftest assert-expr is do-report testing]]

            [jackdaw.streams.mock :as jsm]
            [jackdaw.streams :as js]
            [jackdaw.streams.protocols :as js.proto]
            [jackdaw.test :as jd.test]
            [jackdaw.test.fixtures :as jd.test.fix]
            [jackdaw.test.transports.kafka :as jdtt.kafka]
            [kinsky.client      :as client]
            [kinsky.async       :as async]
            [jsonista.core :as json]
            [tick.alpha.api :as t]


            [kafka-bus.log :as log :refer [info debug]]
            [kafka-bus.domain :refer [EventBus publish! subscribe defevent event-meta-gen]]
            [kafka-bus.async-test-utils]
            [kafka-bus.kafka :as k]
            [kafka-bus.kafka-manager :as km]
            [kafka-bus.kafka-bus :as kb]
            [kafka-bus.time :as time]
            )
  (:import [java.util Properties]
           [org.apache.kafka.streams TopologyTestDriver]))

(def app-config {"application.id"            "common-infra-kafka-test"
                 "bootstrap.servers"         "localhost:9092"
                 "cache.max.bytes.buffering" "0"})

(defn app-config-for-consumer [name]
  (assoc app-config "application.id" name))


(s/def ::org-id string?) ;(s/and string? #(<= (count %) 16) not-empty))
(s/def ::org-name string?)
(s/def ::logo-url uri?)
(s/def ::updated-by string?)
(s/def ::updated-at ::time/instant)
(s/def ::my-test-entity (s/keys :req-un [::org-id ::org-name ::updated-by ::updated-at]))
(s/def ::event-type #{::my-test-entity-created})
(defevent ::my-test-entity-created ::my-test-entity)

(defn gen-event [org-id]
  (assoc (gen/generate (s/gen ::my-test-entity-created)) :org-id org-id))

(deftest ^:integration test-kafka-event-bus-1-consumer
  (testing "1 topic 1 producer that publish 1 event to 1 consumer, no filtering"
    (let [topic-name (str "topic-" (rand-int 1000))
          kafka-event-bus (kb/kafka-event-bus "kafka event bus" app-config (k/topic-config topic-name) (k/consumer-config "myapp-id" "localhost:9092") (map k/topic-config [topic-name]) :org-id)
          collector (atom [])
          streams (subscribe kafka-event-bus (fn [x] (info {:key (:key x)} (str "Got event from " (:name kafka-event-bus)))(swap! collector conj x)))]
      (publish! kafka-event-bus (gen-event "leclerc") )
      (is (eventually (<= 1 (count @collector)) :max-elapsed 5000))
      (js/close streams)
      (km/delete-topics [topic-name])))
  (testing "1 topic 1 producer and 1 subscriber that filter through a key"
    (let [topic-name (str "topic-" (rand-int 1000))
          kafka-event-bus (kb/kafka-event-bus "kafka event bus" app-config (k/topic-config topic-name) (app-config-for-consumer "myapp-id") (map k/topic-config [topic-name]) :org-id )
          collector (atom [])
          streams (subscribe kafka-event-bus "leclerc" (fn [x] (info {:key (:key x)} (str "Got filtered event on key \"leclerc\" from " (:name kafka-event-bus)))(swap! collector conj x)))]
      (publish! kafka-event-bus (gen-event "leclerc") )
      (is (eventually (<= 1 (count @collector)) :max-elapsed 15000))
      (js/close streams)
      (km/delete-topics [topic-name])))
  (testing "1 topic 1 producer and 2 subscriber that filter through a key should get 1 because same app-id"
    (let [topic-name (str "topic-" (rand-int 1000))
          kafka-event-bus (kb/kafka-event-bus "kafka event bus" app-config (k/topic-config topic-name) (app-config-for-consumer "myapp-id") (map k/topic-config [topic-name]) :org-id )
          collector (atom [])
          streams-1 (subscribe kafka-event-bus "leclerc" (fn [x] (info {:key (:key x)} (str "Got filtered event on key \"leclerc\" from " (:name kafka-event-bus)))(swap! collector conj x)))
          streams-2 (subscribe kafka-event-bus "leclerc" (fn [x] (info {:key (:key x)} (str "Got filtered event on key \"leclerc\" from " (:name kafka-event-bus)))(swap! collector conj x)))]
      (publish! kafka-event-bus (gen-event "leclerc") )
      (is (eventually (<= 1 (count @collector)) :max-elapsed 15000))
      (js/close streams-1)(js/close streams-2)
      (km/delete-topics [topic-name]))))

(deftest ^:integration test-kafka-event-bus-n-consumers
  (testing "1 topic 1 producer to 3 buses that publish 5 events to 3 consumers"
    (let [topic-name (str "topic-" (rand-int 1000))
          kafka-event-bus-1 (kb/kafka-event-bus "kafka event bus 1" app-config (k/topic-config topic-name) (app-config-for-consumer "consumer-1") (map k/topic-config [topic-name]) :org-id)
          kafka-event-bus-2 (kb/kafka-event-bus "kafka event bus 2" app-config (k/topic-config topic-name) (app-config-for-consumer "consumer-2") (map k/topic-config [topic-name]) :org-id)
          kafka-event-bus-3 (kb/kafka-event-bus "kafka event bus 3" app-config (k/topic-config topic-name) (app-config-for-consumer "consumer-3") (map k/topic-config [topic-name]) :org-id)
          collector (atom [])
          streams-1 (subscribe kafka-event-bus-1 (fn [x] (info {:key (:key x)} (str "Got event from " (:name kafka-event-bus-1)))(swap! collector conj x)))
          streams-2 (subscribe kafka-event-bus-2 (fn [x] (info {:key (:key x)} (str "Got event from " (:name kafka-event-bus-2)))(swap! collector conj x)))
          streams-3 (subscribe kafka-event-bus-3 (fn [x] (info {:key (:key x)} (str "Got event from " (:name kafka-event-bus-3)))(swap! collector conj x)))]
      (publish! kafka-event-bus-1 (gen-event "leclerc") )
      (publish! kafka-event-bus-1 (gen-event "leclerc") )
      (publish! kafka-event-bus-2 (gen-event "leclerc") )
      (publish! kafka-event-bus-3 (gen-event "leclerc") )
      (publish! kafka-event-bus-3 (gen-event "leclerc") )
      (is (eventually (<= 15 (count @collector)) :max-elapsed 10000))
      (js/close streams-1)
      (js/close streams-2)
      (js/close streams-3)
      (km/delete-topics [topic-name]))))

(comment
  (def p (client/producer app-config (client/keyword-serializer) (client/json-serializer)))
  (client/send! p "topic-test" :my-key (gen/generate (s/gen ::my-test-entity-created)))
  (let [c (client/consumer {:bootstrap.servers "localhost:9092"
                            :group.id          "mygroup"}
                           (client/keyword-deserializer)
                           (client/edn-deserializer))]
    (client/subscribe! c "account")
    (client/poll! c 100)))
