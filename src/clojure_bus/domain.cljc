(ns clojure-bus.domain
  (:require [#?(:clj  clojure.spec.gen.alpha
                :cljs cljs.spec.gen.alpha) :as gen]
            [#?(:clj  clojure.spec.alpha
                :cljs cljs.spec.alpha) :as s]
            [clojure.test.check.generators :as check-gen]
            [tick.alpha.api :as t]

            [clojure-bus.time :as time]
            [clojure-bus.uuid :as uuid]
            )
  #?(:clj (:import [java.time Instant ZonedDateTime OffsetDateTime LocalDateTime LocalDate LocalTime ZoneId ZoneOffset Duration Period]))

  ;; (:refer-clojure :exclude [range iterate format max min]) ;; for java-time to exclude conflicting fn name
  )



(defprotocol EventBus
  "A simple pub/sub interface for publishing event indexed with key k (the key value must be extracted from the event and is up to the implementation, this ensure homogeneity of the key extraction from the event)"
  (subscribe
    [_ f]
    [_ k f] "Subscribe to events, optionally filtered with the key k (usually the org-ref), then callback the function f, the return of the subscribe function is implementation dependant")
  (publish! [_ event] "Publish the event to that bus, the key will be extracted depending on the implementation (usually through a function given at construction or hardcoded)")
  (stop [_] "Lifecycle operation as event bus is likely in need of some cleanup after use (subscribers for instance...)"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Command Spec                                   ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare create-map)
(defmacro create-map
          "Utility macro that create a map from a list of symbols that are in scope"
          [& syms]
          (zipmap (map keyword syms) syms))

(defmulti command-type :command-type)
(s/def ::command (s/multi-spec command-type :command-type))

;;command are implemented in domain project like:
;;  (defmethod command-type ::create-organization-command [_]
;;    (s/keys :req-un [::organization ::command]))
;; ::command spec took one of the command define with the multimethod

(s/def ::command-type keyword?)
(s/def ::command-id uuid?)
(s/def ::correlation-id uuid?)
(s/def ::org-ref string?)
(s/def ::issued-at ::time/instant)
(s/def ::issued-by string?)
(s/def ::received-at ::time/instant)
(s/def ::source string?)
(s/def ::container string?)

(s/def ::command-metadata (s/keys :req-un [::command-type ::command-id ::org-ref ::issued-at ::issued-by]
                                  :opt-un [::source ::container]))

(defn command-meta-gen
      "command metadata generator with fixed command-type"
      [command-type]
      (check-gen/let [command-type (check-gen/return command-type)
                      command-id (s/gen ::command-id)
                      org-ref (s/gen ::org-ref)
                      issued-at (check-gen/one-of [(check-gen/return (t/now)) (s/gen ::issued-at)])
                      issued-by (s/gen ::issued-by)
                      source (s/gen ::source)
                      container (s/gen ::container)]
                     (create-map command-type command-id org-ref issued-at issued-by source container)))

(defn command-metadata [{:keys [command-id command-type issued-at correlation-id] :as metadata}]
  (merge {:command-id     (or command-id (uuid/random))
          :issued-at      (or issued-at (t/instant))
          :correlation-id (or correlation-id (uuid/random))} metadata))

(defmacro defcommand
          "define a command with first arg the ns keyword of the command then the ns keyword of the spec"
          [cmdk datak]
          `(do (s/def ~cmdk
                 (s/with-gen (s/merge ::command-metadata (s/keys :req-un [~datak]))
                             #(gen/fmap (fn [[command-meta# data#]]
                                            (assoc command-meta# (keyword (name ~datak)) data#))
                                        (gen/tuple (command-meta-gen ~cmdk) (s/gen ~datak)))))
               (defmethod command-type ~cmdk [_#] (s/get-spec ~cmdk))

               (defn ~(symbol (str "->" (name cmdk))) ~(str "Create a command of type " cmdk " with metadata (complementing the missing optional data, only :org-ref and :issued-by are mandatory keys in metadata) and data " datak) [metadata# data#]
                 (let [merged-metadata# (command-metadata (assoc metadata# :command-type ~cmdk))]
                   (s/assert* ::command-metadata merged-metadata#)
                   (s/assert* ~datak data#)
                   (assoc merged-metadata# (keyword (name ~datak)) data#)))

               (defn ~(symbol (str (name cmdk) "-gen")) ~(str "Return a generator for the command type " cmdk " and data " datak) []
                 (check-gen/let [meta# (command-meta-gen ~cmdk)
                                 data# (s/gen ~datak)]
                   (assoc meta# (keyword (name ~datak)) data#)))
               ~cmdk))

  (defn commands "return all the commands declared in scope" []
  (-> command-type methods keys set))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query Spec                                     ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::query-type keyword?)
(s/def ::query-id uuid?)
(s/def ::query any?)

(defmulti query-type :query-type)

(s/def ::query-metadata (s/keys :req-un [::query-type ::query-id ::org-ref ::issued-at ::issued-by]
                                :opt-un [::query ::source ::received-at]))

(defn query-metadata [{:keys [query-id query-type issued-at correlation-id] :as metadata}]
  (merge {:query-id       (or query-id (uuid/random))
          :issued-at      (or issued-at (t/instant))
          :correlation-id (or correlation-id (uuid/random))} metadata))

(defn query-meta-gen
      "query metadata generator with fixed command-type"
      [query-type]
      (check-gen/let [query-type (check-gen/return query-type)
                      query-id (s/gen ::query-id)
                      org-ref (s/gen ::org-ref)
                      issued-at (check-gen/one-of [(check-gen/return (t/now)) (s/gen ::issued-at)])
                      issued-by (s/gen ::issued-by)
                      received-at (s/gen ::received-at)
                      source (s/gen ::source)
                      query (s/gen ::query)]
                     (create-map query-type query-id org-ref issued-at issued-by received-at source query)))

(defmacro defquery
          "define a query with first arg the ns keyword of the query then the keyword of the spec"
          [queryk]
          `(do (s/def ~queryk
                 (s/with-gen ::query-metadata
                             #(query-meta-gen ~queryk)))
               (defmethod query-type ~queryk [_#] (s/get-spec ~queryk))

               (defn ~(symbol (str "->" (name queryk))) ~(str "Create a query of type " queryk " with metadata (complementing the missing optional data, only :org-ref and :issued-by are mandatory keys in metadata) and data ") [metadata# data#]
                 (let [merged-metadata# (query-metadata (assoc metadata# :query-type ~queryk))]
                   (s/assert* ::query-metadata merged-metadata#)
                   (assoc merged-metadata# :query data#)))

               (defn ~(symbol (str (name queryk) "-gen")) ~(str "Return a generator for the query type " queryk) []
                 (check-gen/let [meta# (query-meta-gen ~queryk)
                                 data# (s/gen ::query)]
                   (assoc meta# :query data#)))
               ~queryk))

(defn queries "return all the queries declared in scope" []
  (-> query-type methods keys set))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Event Spec                                     ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::event-type keyword?)
(s/def ::event-id uuid?)
(s/def ::from-command-ref uuid?)
(s/def ::from-query-ref uuid?)
(s/def ::published-at ::time/instant)
(s/def ::published-by string?)
(s/def ::from-command ::command-metadata)
(s/def ::from-query ::query-metadata)

(defmulti event-type :event-type)
(s/def ::event (s/multi-spec event-type :event-type))

(s/def ::event-metadata (s/keys :req-un [::event-type ::event-id ::org-ref ::correlation-id ::published-at ::published-by]
                                :opt-un [::container ::source ::from-command-ref ::from-query-ref ::from-command ::from-query]))

(defn event-meta-gen
      "event metadata generator with fixed event-type"
      [event-type]
      (check-gen/let [event-type (check-gen/return event-type)
                      event-id (s/gen ::event-id)
                      correlation-id (s/gen ::correlation-id)
                      org-ref (s/gen ::org-ref)
                      published-at (check-gen/one-of [(check-gen/return (t/now)) (s/gen ::published-at)])
                      published-by (s/gen ::published-by)
                      container (s/gen ::container)
                      source (s/gen ::source)
                      from-command-ref (s/gen ::from-command-ref)
                      from-query-ref (s/gen ::from-query-ref)]
                     (create-map event-type event-id correlation-id org-ref published-at published-by container source from-command-ref)))

(defn event-metadata
  [{:keys [event-id event-type published-by published-at org-ref container source correlation-id from-command-ref from-query-ref from-command from-query] :as metadata}]
  (let [from-command-ref (or from-command-ref (:command-id metadata))
        from-query-ref (or from-query-ref (:query-id metadata))]
    (merge {:event-id       (or event-id (uuid/random))
            :published-at   (t/instant)
            :published-by   (or published-by (or (:issued-by from-command) (:issued-by from-query) (:issued-by metadata))) ;;in case of cmd or query we take the issued-by ;to delete after refactor
            :correlation-id (or correlation-id (uuid/random))} ;to delete after refactor
           (select-keys from-command [:org-ref :correlation-id :source :container])
           (select-keys from-query [:org-ref :correlation-id :source :container])
           metadata)))

(defmacro defevent
          "define a event with first arg the ns keyword of the event then the ns keyword of the spec"
          [evtk datak]
  `(do
     (s/def ~evtk
       (s/with-gen (s/merge ::event-metadata (s/keys :req-un [~datak]))
         #(gen/fmap (fn [[event-meta# org#]] (assoc event-meta# (keyword (name ~datak)) org#))
                    (gen/tuple (event-meta-gen ~evtk) (s/gen ~datak)))))

     (defmethod event-type ~evtk [_#] (s/get-spec ~evtk))

     (defn ~(symbol (str "->" (name evtk)))
       ~(str "Create an event of type " evtk " with metadata (complementing the missing optional data, only :org-ref and :published-by are mandatory keys in metadata) and entity " datak)
       [metadata# entity#]
       (let [merged-metadata# (event-metadata (assoc metadata# :event-type ~evtk))]
         (s/assert* ::event-metadata merged-metadata#)
         (s/assert* ~datak entity#)
         (assoc merged-metadata# :entity entity#)))

     (defn ~(symbol (str (name evtk) "-gen")) ~(str "Return a generator for the event type " evtk " and entity " datak) []
       (check-gen/let [meta# (event-meta-gen ~evtk)
                       entity# (s/gen ~datak)]
         (assoc meta# :entity entity#)))
     ;;return the event keyword
     ~evtk))

(defn events "return all the events declared with defevent that are in scope" []
  (-> event-type methods keys set))

(def realm-name "electre")

(defprotocol Localizable
  "Protocol for handling i18n, l10n"
  (localized-string [this locale]
                    [this locale f]
                    "Returns a string localized with the provided locale and the optionnal format argument"))

(defprotocol Valueable
  "Protocol for common value operations"
  (to-string [this] "Returns a standard string representation of that value that can be parsed with value-of function"))
