(ns clj-zenoh.liveliness
  (:require [clj-zenoh.core :as core]
            [clj-zenoh.utils :as utils])
  (:import [io.zenoh Session]
           [io.zenoh.handlers Handler]
           [io.zenoh.keyexpr KeyExpr]
           [io.zenoh.liveliness Liveliness LivelinessToken LivelinessSubscriberOptions]
           [java.time Duration]))

(set! *warn-on-reflection* true)

(defn declare-token
  "Declare a liveliness token on a key expression.
  The token asserts the liveliness of the application.
  Returns a [[io.zenoh.liveliness.LivelinessToken]].
  Must be closed with .close or use [[with-token]]."
  [^Session session key-expr]
  (let [key-expr (utils/->key-expr key-expr)]
    (.declareToken (.liveliness session) key-expr)))

(defmacro with-token
  "Execute body with a liveliness token in a [[with-open]] binding."
  [[binding token-expr] & body]
  `(with-open [~binding ~token-expr]
     ~@body))

(defn- subscriber-options
  [{:keys [history] :as _opts}]
  (doto (LivelinessSubscriberOptions.)
    (.setHistory (boolean history))))

(defn subscriber
  "Subscribe to liveliness updates.
  Handler must implement [[io.zenoh.handlers.Handler]].
  If a function is provided, it is wrapped with [[clj-zenoh.core/on-sample]].
  Returns a subscriber (HandlerSubscriber or CallbackSubscriber).
  Must be closed with .close.
   
  Options:
  {:history boolean} ; Receive historical liveliness states"
  ([^Session session key-expr handler-or-fn]
   (subscriber session key-expr handler-or-fn nil))
  ([^Session session key-expr handler-or-fn opts]
   (let [liveliness (.liveliness session)
         key-expr (utils/->key-expr key-expr)
         handler (if (fn? handler-or-fn) (core/on-sample handler-or-fn) handler-or-fn)
         sub-opts (subscriber-options opts)]
     (if sub-opts
       (.declareSubscriber ^Liveliness liveliness ^KeyExpr key-expr ^Handler handler ^LivelinessSubscriberOptions sub-opts)
       (.declareSubscriber ^Liveliness liveliness ^KeyExpr key-expr ^Handler handler)))))

(defn get!
  "Query liveliness tokens.
  Handler must implement [[io.zenoh.handlers.Handler]].
  If a function is provided, it is wrapped with [[clj-zenoh.core/on-reply]].
  Returns the value returned by the Handler's receiver().
  
  Options:
  {:timeout-ms int}"
  ([^Session session key-expr handler-or-fn]
   (get! session key-expr handler-or-fn nil))
  ([^Session session key-expr handler-or-fn opts]
   (let [liveliness (.liveliness session)
         key-expr (utils/->key-expr key-expr)
         handler (if (fn? handler-or-fn) (core/on-reply handler-or-fn) handler-or-fn)
         timeout (when-let [ms (:timeout-ms opts)]
                   (Duration/ofMillis ms))]
     (if timeout
       (.get ^Liveliness liveliness ^KeyExpr key-expr ^Handler handler ^Duration timeout)
       (.get ^Liveliness liveliness ^KeyExpr key-expr ^Handler handler)))))
