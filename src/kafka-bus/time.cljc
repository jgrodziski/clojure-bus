(ns kafka-bus.time
  "Time handling.
  The application uses the tick library to manipulate time. This library is built upon java-time classes (and uses cljc
  compatible version on the frontend)."
  #?(:clj
     (:require
       [clojure.spec.gen.alpha :as gen]
       [clojure.spec.alpha :as s]
       [tick.alpha.api :as t]
       [tick.locale-en-us]
       [cljc.java-time.instant :as i :refer [to-epoch-milli]]
       [cljc.java-time.local-date-time :as ldt :refer [of-instant]]
       [cljc.java-time.local-date :as ld]
       [cljc.java-time.local-time :as lt]
       [cljc.java-time.zoned-date-time :as zdt]
       [cljc.java-time.zone-id :as zi :refer [get-available-zone-ids]]
       [java-time :refer [local-date-time local-date local-time zoned-date-time instant]]))
  #?(:cljs
     (:require
       [cljs.spec.gen.alpha :as gen]
       [cljs.spec.alpha :as s]
       [tick.alpha.api :as t]
       [tick.locale-en-us]
       ["@js-joda/locale_en-us" :as js-joda-locale]
       [cljc.java-time.instant :as i :refer [to-epoch-milli]]
       [cljc.java-time.local-date-time :as ldt :refer [of-instant]]
       [cljc.java-time.local-date :as ld]
       [cljc.java-time.local-time :as lt]
       [cljc.java-time.zoned-date-time :as zdt]
       [cljc.java-time.zone-id :as zi :refer [get-available-zone-ids]]
       [java.time :refer [Instant ZonedDateTime OffsetDateTime LocalDateTime LocalDate LocalTime ZoneId ZoneOffset Duration Period]]
       [java.time.format :refer [DateTimeFormatter]]
       [java.time.temporal :refer [TemporalQueries]]
       ))
  #?(:clj (:import [java.time Instant ZonedDateTime OffsetDateTime LocalDateTime LocalDate LocalTime ZoneId ZoneOffset Duration Period])))

(defn UTC-zoned-date-time? [x]
      (and (instance? ZonedDateTime x)
           (.equals (t/zone "UTC") (t/zone x))))

(defn duration? [x]
      (instance? Duration x))

(defn period? [x]
      (instance? Period x))

(defn zoned-date-time? [x]
      (and (instance? ZonedDateTime x)))

(defn offset-date-time? [x]
      (instance? OffsetDateTime x))

(defn local-date-time? [x]
      (instance? LocalDateTime x))

(defn local-date? [x]
      (instance? LocalDate x))

(defn local-time? [x]
      (instance? LocalTime x))

(defn zone-id? [x]
      (instance? ZoneId x))

(defn instant? [x]
      (instance? Instant x))

#?(:clj (defn java-sql-date? [x]
              (instance? java.sql.Date x)))

#?(:cljs (defn java-sql-date? [x]
               false))


(def UTC (t/zone "UTC"))
;(def OffsetUTC (ZoneOffset/UTC))

(def all-zone-ids
  (delay
    (get-available-zone-ids)))

(defn ^:dynamic *zone-ids*
      "Dynamically bind this to choose which time zones to use in generators."
      []
      (gen/one-of [(gen/return (t/zone "UTC"))
                   (s/gen @all-zone-ids)]))

(defn- interop-local-date-time [y m d h mm ss]
       #?(:clj (local-date-time y m d h mm ss))
       #?(:cljs (ldt/of y m d h mm ss)))

(s/def ::past (s/int-in (to-epoch-milli (t/instant (t/in (interop-local-date-time 2001 1 1 00 00 00) UTC)))
                        (to-epoch-milli (t/instant (t/in (interop-local-date-time 2010 12 31 00 00 00) UTC)))))

(s/def ::past-and-future (s/int-in (to-epoch-milli (t/instant (t/in (interop-local-date-time 2011 1 1 00 00 00) UTC)))
                                   (to-epoch-milli (t/instant (t/in (interop-local-date-time 2030 12 31 23 59 59) UTC)))))

(s/def ::future (s/int-in (to-epoch-milli (t/instant (t/in (interop-local-date-time 2031 1 1 00 00 00) UTC)))
                          (to-epoch-milli (t/instant (t/in (interop-local-date-time 2040 12 31 23 59 59) UTC)))))

(defn ^:dynamic *period*
      "Dynamically bind this to choose the range of your generated dates."
      []
      (s/gen ::past-and-future))

(s/def ::zone-id (s/with-gen zone-id? *zone-ids*))

(s/def ::UTC-zoned-date-time (s/with-gen zoned-date-time?
                                         #(gen/fmap (fn [ms]
                                                        (t/in (t/instant ms) UTC))
                                                    (*period*))))

(s/def ::UTC-local-date-time (s/with-gen local-date-time?
                                         #(gen/fmap (fn [ms]
                                                        (of-instant (t/instant ms) UTC))
                                                    (*period*))))

(s/def ::instant (s/with-gen instant?
                             #(gen/fmap (fn [ms] (t/instant ms)) (*period*))))

(s/def ::local-date (s/with-gen local-date?
                                #(gen/fmap (fn [local-date-time]
                                               (t/date local-date-time))
                                           (s/gen ::UTC-local-date-time))))

;; Time constructor functions.
;; Those are mainly wrapper around the tick constructor functions.
;; Note that those are *constructor* functions intended to build specific instances
;; of date objects from their arguments.
;; When trying to create a date from a string or nothing (ie: (instant)) then you 
;; may need to call the tick coercion functions directly.
;; Tick has a concept of both constructor / coercion functions. Constructor functions
;; used their arguments. Coercion function take a date *representation* and coerce it
;; to the correct type.
;; Example
;;    tick/time: coercion function wich returns a LocalTime and takes either no
;;    or a string or any other type supporting coercion
;;    tick/new-time: constructor function wich returns a LocalTime wich has a multi-arity
;;    signature [d m h s]
(defn ->instant
      ([]
       (t/instant))
      ([epoch]
       (t/instant epoch)))

(defn ->local-date
      [year month day]
      (t/new-date year month day))

(defn local-date->year [local-date]
      (t/year local-date))

(defn local-date->month [local-date]
      (t/month local-date))

(defn local-date->day-of-month [local-date]
      (t/day-of-month local-date))

(defn ->local-date-time
      ([y m d h mn s]
       (->local-date-time y m d h mn s 0))
      ([y m d h mn s n]
       (let [time (t/new-time h mn s n)
             date (t/new-date y m d)]
            (t/on time date))))

;; Note the different arity clj vs cljs. Nanos are ignored on the frontend and
;; thus are not required when creating a new zoned-date-time instance.
(defn ->zoned-date-time
      [y m d h mn s n o z]
      (-> (t/new-time h mn s n)
          (t/on (t/new-date y m d))
          (t/offset-by o)
          t/zoned-date-time                                 ;; coercion
          (t/in (t/zone z))))

(defn n-month-from-now [n]
      (t/+ (t/new-date) (t/new-period n :months)))

#?(:cljs
   (set! js/JSJodaLocale js-joda-locale))
(def yyyy-MM-dd (tick.format/formatter "yyyy-MM-dd"))


#?(:clj (defn annee-mois-jour-str [x]
              (when x
                    (cond (or (local-date? x) (local-date-time? x)) (t/format yyyy-MM-dd x)
                          (instant? x) (t/format yyyy-MM-dd (t/date x))
                          (inst? x) (t/format yyyy-MM-dd (t/date (t/instant x)))
                          (java-sql-date? x) (t/format yyyy-MM-dd (.toLocalDate x))
                          :else (throw (ex-info "Argument must be a local date or an instant" {:arg x}))))))

#?(:cljs (defn annee-mois-jour-str [x]
               (when x
                     (cond (or (local-date? x) (local-date-time? x)) (t/format yyyy-MM-dd x)
                           (instant? x) (t/format yyyy-MM-dd (t/date x))
                           (inst? x) (t/format yyyy-MM-dd (t/date (t/instant x)))
                           :else (throw (ex-info "Argument must be a local date or an instant" {:arg x}))))))

(defn format-date [fmt x]
      (when x
            #?(:clj
               (cond (or (local-date? x) (local-date-time? x)) (t/format (t/format fmt) x)
                     (instant? x) (t/format fmt (t/date x))
                     (inst? x) (t/format fmt (t/date (t/instant x)))
                     :else (throw (ex-info "Argument must be a local date or an instant" {:arg x})))
               :cljs
               (cond (or (local-date? x) (local-date-time? x)) (t/format (tick.format/formatter fmt) x)
                     (instant? x) (t/format (tick.format/formatter fmt) (t/date x))
                     (inst? x) (t/format (tick.format/formatter fmt) (t/date (t/instant x)))
                     :else (throw (ex-info "Argument must be a local date or an instant" {:arg x}))))))

(defn parse-date-time-UTC
      "parse a string in the ISO date time format yyyy-MM-ddThh:mm:ss, return a java.time.LocalDateTime object"
      [s]
      (t/date-time s))

(defn str->local-date-time [x]
      "Retourne une LocalDateTime à partir de plusieurs format String :
      * dd-MM-yyy
      * yyyy-MM-dd
      * dd/MM/yyyy
      * yyyy/MM/dd
      * yyyy-MM-dd'T'HH:mm
      * yyyy-MM-dd'T'HH:mm:ss
      "
      (let [formats ["dd-MM-yyyy['T'HH:mm[:ss]]" "yyyy-MM-dd['T'HH:mm[:ss]]" "dd/MM/yyyy['T'HH:mm[:ss]]" "yyyy/MM/dd['T'HH:mm[:ss]]"]]
           (if-let [resultat (some (fn [fmt]
                                       (try
                                         #?(:clj  (if (> (count x) 10) (local-date-time fmt x) (.atStartOfDay (local-date fmt x)))
                                            :cljs (.parse (. DateTimeFormatter ofPattern fmt) x
                                                          #js {:queryFrom
                                                               (fn [temporal]
                                                                   (let [date (.query temporal (.localDate TemporalQueries))
                                                                         time (.query temporal (.localTime TemporalQueries))]
                                                                        (if (= time nil) (.atStartOfDay date) (.atTime date time))))}))
                                         (catch #?(:clj Exception :cljs js/Object) _)))
                                   formats)]
                   resultat
                   (throw (ex-info (str "La Date : " x " ne peut être parsée car elle ne correspond à aucun format accepté : " formats)
                                   {:date-recue       x
                                    :formats-acceptes formats}))))
      )


