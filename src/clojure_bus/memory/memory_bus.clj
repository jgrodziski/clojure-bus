(ns clojure-bus.memory.memory-bus
  (:require [manifold.stream :as s :refer [consume]]
            [manifold.bus :as b]

            [clojure-bus.utils.log :as log]
            [clojure-bus.bus :refer [EventBus publish! subscribe]]))

(def DEFAULT_STREAM_BUFFER 2048)

(defrecord InMemoryEventBus [name key-extract-fn filter-fn bus]
  EventBus
  (publish! [_ event]
    (if (filter-fn event)
      (let [k (key-extract-fn event)]
        (b/publish! bus k event)
        (log/debug {:service ::publish :key k :event event :bus-name name} "Event published through InMemoryEventBus"))
      (log/debug {:service ::publish :event event :bus-name name} "Discard event of InMemoryEventBus")))
  (subscribe [_ f]
    (throw (UnsupportedOperationException. "Can't subscribe without a key to in-memory bus")))
  (subscribe [_ k f]
    (let [sub-stream (b/subscribe bus k) ;; subscribing to a bus returns a manifold stream
          exhausted? (s/consume f sub-stream)] ;; consuming a stream with a function returns a deferred
      (log/info {:service ::subscribe :key k :bus-name name} (format "Subscribed to events with key %s through InMemoryEventBus %s" k name))
      {:stream sub-stream
       :exhausted? exhausted?
       :stop (fn []
               (s/close! sub-stream))})))

(comment 
  (defn fn-name [f]
    (as-> (str f) $
      (clojure.main/demunge $)
      (or (re-find #"(.+)--\d+@" $)
          (re-find #"(.+)@" $))
      (last $))))

(defn in-memory-event-bus
  ([key-extract-fn]
   (in-memory-event-bus "InMemoryBus" key-extract-fn))
  ([name key-extract-fn]
   (in-memory-event-bus name key-extract-fn identity))
  ([name key-extract-fn filter-fn]
   (in-memory-event-bus name key-extract-fn filter-fn DEFAULT_STREAM_BUFFER))
  ([name key-extract-fn filter-fn buffer-size]
   (log/info {:service ::in-memory-event-bus :bus-name name :buffer-size buffer-size} (format "Create InMemoryEventBus %s" name))
   (let [stream-generator-fn #(s/stream buffer-size) ;; subscribing to an event-bus returns the stream created by this function
         bus (->InMemoryEventBus name
                                 key-extract-fn ;; manifold buses use the concept of topics. this function will dispatch to the correct topic
                                 filter-fn
                                 (b/event-bus stream-generator-fn))]
     (log/info {:service ::in-memory-event-bus :bus-name name} (format "InMemoryEventBus %s created" name))
     bus)))

(comment
  ;;ManifoldBus is used to wrap the event bus with the same interface as the manifold/bus
  ;;particularly to return stream instead of invoking a callback function
  (defprotocol ManifoldBus
    (publish! [bus topic message])
    (subscribe [bus topic])
    (topic->subscribers [bus])
    (downstream [bus topic]))
  
  (def source (s/stream))
  (def filtered (s/filter even? source))
  (s/consume prn filtered)
  (s/put! source 1)
  (s/put! source 2)
  (s/put! source 3)
  (s/put! source 4)
  (s/put! source 6))
