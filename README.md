# kafka-bus

An implementation of the Bus protocol with Kafka

```clojure
(defprotocol EventBus
  "A simple pub/sub interface for publishing event indexed with key k (the key value must be extracted from the event and is up to the implementation, this ensure homogeneity of the key extraction from the event)"
  (subscribe
    [_ f]
    [_ k f] "Subscribe to events, optionally filtered with the key k (usually the org-ref), then callback the function f, the return of the subscribe function is implementation dependant")
  (publish! [_ event] "Publish the event to that bus, the key will be extracted depending on the implementation (usually through a function given at construction or hardcoded)")
  (stop [_] "Lifecycle operation as event bus is likely in need of some cleanup after use (subscribers for instance...)"))

```
