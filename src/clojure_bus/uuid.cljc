(ns clojure-bus.uuid
  #?(:cljs (:require [cljs-uuid-utils.core :as uuid]))
  #?(:clj (:import (java.util UUID))))

(defn random []
  #?(:clj (UUID/randomUUID))
  #?(:cljs (uuid/make-random-uuid)))

(defn of [s]
  #?(:clj (UUID/fromString s))
  #?(:cljs (uuid/make-uuid-from s)))

(defn to-string [uuid]
  #?(:clj (.toString uuid))
  #?(:cljs (uuid/uuid-string uuid)))
