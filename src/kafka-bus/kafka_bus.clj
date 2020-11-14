(ns kafka-bus.kafka-bus
  (:require
   [jackdaw.streams :as js]

   [kafka-bus.log :as log]
   [kafka-bus.bus :refer [EventBus subscribe publish!]]
   [kafka-bus.kafka-manager :as km]
   [kafka-bus.kafka :as k]))

(defrecord KafkaEventBus [name producer-conf prod-topic-conf consumer-conf consumer-topics-conf key-extract-fn streams-store]
  EventBus
  (publish! [_ event]
    (k/publish! producer-conf prod-topic-conf (key-extract-fn event) event)
    (log/info {:service ::publish :topic (:topic-name prod-topic-conf) :key (key-extract-fn event):event event :bus-name name} "Event published on Kafka through KafkaEventBus")
    event)
  (subscribe [_ f]
    (let [streams (k/start-event-streams consumer-conf consumer-topics-conf k/topology-for-each f)]
      (log/info {:service ::subscribe :consumer-id (get consumer-conf "application.id") :topics (map :topic-name consumer-topics-conf) :bus-name name} "Events Kafka stream processor subscribed through KafkaEventBus")
      (swap! streams-store conj streams)
      streams))
  (subscribe [_ k f]
    (let [streams (k/start-event-streams consumer-conf consumer-topics-conf k/topology-filter k f)]
      (log/info {:service ::subscribe :topics (map :topic-name consumer-topics-conf) :key k :bus-name name} (str "Events Kafka stream processor subscribed for key " k))
      (swap! streams-store conj streams)
      streams))
  (stop [_]
    (log/info {:service ::stop :topics (map :topic-name consumer-topics-conf) :bus-name name} (str "Stop Kafka Event Bus and close " (count @streams-store) " streams"))
    (for [streams @streams-store]
      (js/close streams))))

(defn kafka-event-bus [name producer-conf producer-topic-conf consumer-conf consumer-topics-conf key-extract-fn]
  (log/info {:service ::kafka-event-bus
             :producer-conf producer-conf
             :producer-topic-conf producer-topic-conf
             :consumer-conf consumer-conf
             :consumer-topics-conf consumer-topics-conf
             :bus-name name} (str "Create KafkaEventBus " name))
  (km/topics-exist-or-create producer-conf [producer-topic-conf])
  (km/topics-exist-or-create consumer-conf consumer-topics-conf)
  (let [b (->KafkaEventBus name producer-conf producer-topic-conf consumer-conf consumer-topics-conf key-extract-fn (atom []))]
    (log/info {:service ::kafka-event-bus :bus-name name} (str "KafkaEventBus " name " created"))
    b))
