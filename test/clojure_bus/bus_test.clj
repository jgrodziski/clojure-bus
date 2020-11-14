(ns clojure-bus.bus-test
  (:require  [clojure.test :as t :refer [deftest testing]]
             [clojure.spec.alpha :as s]
             [clojure.spec.gen.alpha :as gen]
             [clojure.test :refer [deftest assert-expr is do-report testing]]
             [jackdaw.streams :as js]
             [manifold.stream :as manifold]

             [clojure-bus.log :refer [info]]
             [clojure-bus.multicast-bus :refer [multicast-event-bus connect]]
             [clojure-bus.domain :refer [defevent subscribe publish!]]
             [clojure-bus.kafka :as k]
             [clojure-bus.async-test-utils :refer :all]
             [clojure-bus.kafka-bus :refer [kafka-event-bus]]
             [clojure-bus.memory-bus :refer [in-memory-event-bus]]
             ))

(def app-config {"application.id"            "common-infra-kafka-test"
                 "bootstrap.servers"         "localhost:9092"
                 "cache.max.bytes.buffering" "0"})

(defn consumer-config [name]
  (assoc app-config "application.id" name))

(s/def ::org-id string?) ;(s/and string? #(<= (count %) 16) not-empty))
(s/def ::org-name string?)
(s/def ::logo-url uri?)
(s/def ::updated-by string?)
(s/def ::my-test-entity (s/keys :req-un [::org-id ::org-name ::updated-by]))
(s/def ::event-type #{::my-test-entity-created})
(defevent ::my-test-entity-created ::my-test-entity)

(defn gen-event [org-id]
  (assoc (gen/generate (s/gen ::my-test-entity-created)) :org-id org-id))

(deftest ^:integration test-multicast-kafka-in-memory-bus
  (testing "Send events through multicast should be received in each bus connected to the multicast one"
    (let [multicast (multicast-event-bus)
          kafka (kafka-event-bus "Kafka event bus" app-config (k/topic-config "topic-1") (consumer-config "consumer-1") (map k/topic-config ["topic-1"]) :org-id)
          in-memory-1 (in-memory-event-bus "in memory 1" :org-id)
          in-memory-2 (in-memory-event-bus "in memory 2" :org-id)
          collector-kafka (atom [])
          collector-in-1 (atom [])
          collector-in-2 (atom [])
          streams-1 (subscribe kafka                 (fn [x] (info {:key (:key x)} (str "Got event from " (:name kafka)))         (swap! collector-kafka   conj x)))
          streams-2 (subscribe in-memory-1 "leclerc" (fn [x] (info {:key (:key x)} (str "Got event from " (:name in-memory-1)))   (swap! collector-in-1    conj x)))
          streams-3 (subscribe in-memory-2 "leclerc" (fn [x] (info {:key (:key x)} (str "Got event from " (:name in-memory-2)))   (swap! collector-in-2    conj x)))]
      (connect multicast kafka)
      (connect multicast in-memory-1)
      (connect multicast in-memory-2)

      (publish! multicast (gen-event "leclerc") )
      (publish! multicast (gen-event "leclerc") )
      (publish! multicast (gen-event "leclerc") )
      (publish! multicast (gen-event "nimp") );only kafka should receive this one

      (is (eventually (<= 3 (count @collector-in-1)) :max-elapsed 500))
      (is (eventually (<= 3 (count @collector-in-2)) :max-elapsed 500))
      (is (eventually (<= 4 (count @collector-kafka)) :max-elapsed 10000))
      (js/close streams-1)
      )))
