(ns clojure-bus.kafka-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test :as t :refer [deftest testing]]
            [testit.core :refer [fact]]
            [clojure-bus.time :as time]

            [clojure-bus.domain :refer [defevent]]
            [clojure-bus.json :as json]
            [clojure-bus.kafka :as k]))

(s/def ::org-id uuid?) ;(s/and string? #(<= (count %) 16) not-empty))
(s/def ::org-name string?)
(s/def ::logo-url uri?)
(s/def ::updated-by string?)
(s/def ::updated-at ::time/instant)

(s/def ::my-test-entity (s/keys :req-un [::org-id ::org-name ::updated-by ::updated-at]))
(s/def ::event-type #{::my-test-entity-created})
(defevent ::my-test-entity-created ::my-test-entity)

(defn gen-event []
  (gen/generate (s/gen ::my-test-entity-created)))

(deftest test-kafka-json-bytes-encoding
  (testing "Reversibility of encoding/decoding functions"
    (let [event (gen-event)]
      (fact "reverse is the same" (-> event
                                      k/data->transit-json-bytes
                                      k/transit-json-bytes->data
                                      json/typify-string) => event))))
