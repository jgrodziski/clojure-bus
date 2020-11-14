(ns clojure-bus.async-test-utils
  (:require [clojure.test :refer [deftest assert-expr is do-report testing]]))

(defmethod assert-expr 'eventually [_ form]
  "Asserts that given predicate is eventually true.
   It will try predicate multiple times with 50 ms delay between retries.
   After max-elapsed (default 2000 ms) it will return failure in same way normal clojure.test assertion does."
  (let [[_ [pred actual expected] & {:keys [max-elapsed] :or {max-elapsed 2000} :as options}] form]
   `(let [[success?# last-values# total-time#] (loop [time-elapsed# 0]
                                                 (let [values# (list ~actual ~expected)
                                                       result# (apply ~pred values#)]
                                                   (if result#
                                                     [true values# time-elapsed#]
                                                     (if (< time-elapsed# ~max-elapsed)
                                                       (do (Thread/sleep 50) (recur (+ 50 time-elapsed#)))
                                                       [false values# time-elapsed#]))))]
      (if success?#
        (do-report {:type :pass
                    :message (str "Predicate passed in " total-time#)
                    :expected '~form
                    :actual (cons ~pred last-values#)})
        (do-report {:type :fail
                    :message (str "Predicate failed after trying multiple times within") ~max-elapsed" ms."
                    :expected '~form,
                    :actual (list '~'not (cons '~pred last-values#))})))))
