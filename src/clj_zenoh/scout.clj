(ns clj-zenoh.scout
  "Clojure wrapper for Zenoh Scout API."
  (:require [clj-zenoh.core :as core]
            [clojure.string :as str])
  (:import [io.zenoh Zenoh]
           [io.zenoh.scouting Scout ScoutOptions Hello]
           [io.zenoh.config WhatAmI]
           [io.zenoh.handlers Callback Handler]))

(set! *warn-on-reflection* true)

(def ^:private what-am-i-map
  {:router WhatAmI/Router
   :peer WhatAmI/Peer
   :client WhatAmI/Client})

(defn- ->what-am-i
  "Convert keyword to [[io.zenoh.config.WhatAmI]]."
  [k]
  (cond
    (instance? WhatAmI k) k
    (contains? what-am-i-map k) (get what-am-i-map k)
    :else (throw (ex-info "Invalid :what-am-i value"
                          {:value k :allowed (keys what-am-i-map)}))))

(defn- what-am-i->keyword
  [^WhatAmI w]
  (when w
    (keyword (str/lower-case (.name w)))))

(defn hello->map
  "Convert [[io.zenoh.scouting.Hello]] to a clojure map."
  [^Hello hello]
  (when hello
    {:what-am-i (what-am-i->keyword (.getWhatAmI hello))
     :zid (str (.getZid hello))
     :locators (vec (.getLocators hello))}))

(defn- ^ScoutOptions ->scout-options
  "Build [[io.zenoh.scouting.ScoutOptions]] from map.
  Accepts:
  {:what-am-i #{:peer :router :client} ; Defaults to #{:peer :router}
   :config {...}}"
  [{:keys [what-am-i config]}]
  (let [^ScoutOptions scout-options (ScoutOptions.)
        what-am-i* (or what-am-i #{:peer :router})]
    (when what-am-i*
      (let [set-enum (into #{} (map ->what-am-i) what-am-i*)]
        (.setWhatAmI scout-options set-enum)))
    (when config
      (.setConfig scout-options (core/config-from-map config)))
    scout-options))

(defn ^Callback on-hello [f]
  (reify Callback
    (run [_ hello]
      (f (hello->map ^Hello hello)))))

(defn ^Scout scout
  "Start a Scout to discover other Zenoh nodes.
  Returns a [[io.zenoh.scouting.Scout]].

  If no `handler-or-fn` provided, returns a Scout where `.getReceiver()` returns a BlockingQueue
  (see: https://eclipse-zenoh.github.io/zenoh-java/io/zenoh/scouting/HandlerScout.html#getReceiver())

  If a `handler-or-fn` is provided, starts a scout with a callback function or Handler.
  If a function is provided, it receives hello maps (see [[on-hello]])
  If a Handler is provided, it must handle [[io.zenoh.scouting.Hello]].
   
  Options:
  {:what-am-i #{:peer :router :client} ; Defaults to #{:peer :router}
   :config {...}} ; Zenoh config map or string"
  ([opts]
   (Zenoh/scout ^ScoutOptions (->scout-options opts)))
  ([opts handler-or-fn]
   (let [^ScoutOptions scout-options (->scout-options opts)]
     (if (fn? handler-or-fn)
       (Zenoh/scout ^Callback (on-hello handler-or-fn) scout-options)
       (Zenoh/scout ^Handler handler-or-fn scout-options)))))

(defmacro with-scout
  "Execute body with a scout in a [[with-open]] binding."
  [[binding opts & [handler]] & body]
  (if handler
    `(with-open [~binding (scout ~opts ~handler)]
       ~@body)
    `(with-open [~binding (scout ~opts)]
       ~@body)))
