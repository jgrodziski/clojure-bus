(ns kafka-bus.log
  (:require
   #?(:clj [cambium.core :as log])))

#?(:clj
   (defn info [d m]
     (log/info d m)))

#?(:clj
   (defn warn [d m]
     (log/warn d m)))

#?(:clj
   (defn error [d m]
     (log/error d m)))

#?(:clj
   (defn debug [d m]
     (log/debug d m)))


#?(:cljs
   (defn info [d m]
     (.log js/console m)))

#?(:cljs
   (defn warn [d m]
     (.warn js/console m)))

#?(:cljs
   (defn error [d m]
     (.error js/console m)))

#?(:cljs
   (defn debug [d m]
     (.debug js/console m))
   )
