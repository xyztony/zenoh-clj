(ns clj-zenoh.querier
  "Clojure wrapper for Zenoh Querier abstraction."
  (:require [clj-zenoh.core :as core]
            [clj-zenoh.utils :as utils])
  (:import [io.zenoh Session]
           [io.zenoh.bytes ZBytes]
           [io.zenoh.handlers Handler]
           [io.zenoh.keyexpr KeyExpr]
           [io.zenoh.query Parameters Querier QuerierOptions Querier$GetOptions Reply]))

(set! *warn-on-reflection* true)

(defn- ->parameters
  "Convert map or string to [[io.zenoh.query.Parameters]]."
  [params]
  (cond
    (instance? Parameters params) params
    (map? params) (Parameters/from ^java.util.Map params)
    (string? params) (Parameters/from ^String params)
    :else (Parameters/from "")))

(defn- ^QuerierOptions querier-options
  "Build [[io.zenoh.query.QuerierOptions]] from options map.
  Accepts:
  {:target :best-matching | :all | :all-complete
   :consolidation-mode :auto | :none | :monotonic | :latest
   :congestion-control :drop | :block
   :priority :realtime | :interactive-high | :interactive-low | :data-high | :data | :data-low | :background
   :timeout <Duration>  ; e.g., (java.time.Duration/ofMillis 1000)
   :express true | false}"
  [{:keys [target consolidation-mode congestion-control priority timeout express] :as opts}]
  (cond-> (QuerierOptions.)
    target (.setTarget (utils/->query-target target))
    consolidation-mode (.setConsolidationMode (utils/->consolidation-mode consolidation-mode))
    congestion-control (.setCongestionControl (utils/->congestion-control congestion-control))
    priority (.setPriority (utils/->priority priority))
    timeout (.setTimeout timeout)
    (some? express) (.setExpress (boolean express))))

(defn querier
  "Declare a Querier on a key expression.
  Returns a [[io.zenoh.query.Querier]].
  Must be closed with [[close!]] or use [[with-querier]].
  
  Options:
  {:target :best-matching
   :consolidation-mode :auto
   :timeout (java.time.Duration/ofMillis 5000)
   :express false
   :congestion-control :block
   :priority :data}"
  ([^Session session key-expression]
   (querier session key-expression nil))
  ([^Session session key-expression opts]
   (let [key-expr (utils/->key-expr key-expression)
         ^QuerierOptions q-opts (querier-options opts)]
     (if q-opts
       (.declareQuerier ^Session session ^KeyExpr key-expr ^QuerierOptions q-opts)
       (.declareQuerier ^Session session ^KeyExpr key-expr)))))

(defmacro with-querier
  "Execute body with a querier in a [[with-open]] try expression."
  [[binding querier-expr] & body]
  `(with-open [~binding ~querier-expr]
     ~@body))

(defn- ^Querier$GetOptions get-options
  "Build [[io.zenoh.query.Querier$GetOptions]] from options map.
  Accepts:
  {:parameters \"a=1;b=2\" | {:a \"1\" :b \"2\"}
   :payload 'query data' | byte-array
   :encoding :application/json | etc
   :attachment 'metadata'}"
  [{:keys [parameters payload encoding attachment] :as opts}]
  (when (seq opts)
    (let [^Querier$GetOptions go (Querier$GetOptions.)]
      (when parameters (.setParameters go (->parameters parameters)))
      (when payload (.setPayload go ^ZBytes (utils/->zbytes payload)))
      (when encoding (.setEncoding go (utils/->encoding encoding)))
      (when attachment (.setAttachment go ^ZBytes (utils/->zbytes attachment)))
      go)))

(defn get!
  "Perform a query with a declared Querier.
  Accepts a custom Handler or function.
  
  If a handler/function is provided:
    Returns the result of the Handler's receiver().
    Function is wrapped with [[core/on-reply]].
    
  If NO handler is provided:
    Returns a BlockingQueue<Optional<Reply>> (Java API default).
    
  Options:
  {:parameters \"key=val\" or {:key \"val\"}
   :payload 'data'
   :encoding :text/plain
   :attachment 'meta'}"
  ([^Querier querier]
   (.get querier (Querier$GetOptions.)))
  ([^Querier querier opts-or-handler]
   (if (or (fn? opts-or-handler) (instance? Handler opts-or-handler))
     (get! querier opts-or-handler nil)
     (let [^Querier$GetOptions opts (get-options opts-or-handler)]
       (if opts
         (.get querier opts)
         (.get querier (Querier$GetOptions.))))))
  ([^Querier querier handler-or-fn opts]
   (let [h (if (fn? handler-or-fn) (core/on-reply handler-or-fn) handler-or-fn)
         ^Querier$GetOptions get-opts (get-options opts)]
     (if get-opts
       (.get querier ^Handler h get-opts)
       (.get querier ^Handler h (Querier$GetOptions.))))))

