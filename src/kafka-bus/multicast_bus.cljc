(ns kafka-bus.multicast-bus
  (:require [clojure.core.async :as async :refer [chan mult tap <! >! go go-loop]]

            [kafka-bus.log :as log :refer [info debug]]
            [kafka-bus.bus :refer [EventBus publish! subscribe]]))

(def DEFAULT_CHAN_BUFFER_SIZE 16)

(defn multicast [out-chan event]
  ;;copy the request/response to the channel
  (debug {:service :multicast :event event} "Multicast event")
  (go
    (async/>! out-chan event)))

(defprotocol Multi
  (connect [_ event-bus]
    "add an event-bus that will receive through the publish! operation all the events that the Multi receive"))

(defrecord MulticastEventBus [name out-chan mult-chan]
  Multi
  (connect [_ bus]
    (info {:service ::connect :from name :to (:name bus)} (str "Connect event bus " (:name bus) " to multicast events from " name))
    (let [bus-chan (chan)];;unbuffered
      (async/tap mult-chan bus-chan)
      (go-loop []
        (let [event (async/<! bus-chan)]
          (debug {:service ::connect :event-type (:event-type event)} (str "Receive the event to forward to " (:name bus)))
          (publish! bus event))
        (recur))))
  EventBus
  (publish! [_ event]
    (multicast out-chan event))
  (subscribe [_ f]
    (throw (UnsupportedOperationException. "Can't subscribe directly to a multicast bus")))
  (subscribe [_ k f]
    (throw (UnsupportedOperationException. "Can't subscribe directly to a multicast bus"))))

(defn multicast-event-bus
  "Create a MulticastEventBus"
  ([] (multicast-event-bus DEFAULT_CHAN_BUFFER_SIZE))
  ([n] (let [out (chan n)
             e (MulticastEventBus. "MulticastEventBus" out (mult out))]
         (info {:service ::multicast-event-bus :buffer-size n} "MulticastEventBus created")
         e)))
