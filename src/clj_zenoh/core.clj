(ns clj-zenoh.core
  "Clojure wrapper for Zenoh Java wrapper/API."
  (:require [charred.api :as json]
            [clojure.string :as str]
            [clj-zenoh.utils :as utils])
  (:import [io.zenoh Config Session Zenoh]
           [io.zenoh.bytes Encoding ZBytes]
           [io.zenoh.handlers Callback Handler]
           [io.zenoh.keyexpr KeyExpr]
           [io.zenoh.pubsub Publisher PublisherOptions PutOptions Subscriber]
           [io.zenoh.qos CongestionControl Reliability]
           [io.zenoh.query GetOptions Query Queryable QueryableOptions Reply Reply$Success Selector]
           [io.zenoh.sample Sample]
           [java.time Instant]
           [java.util Date]
           [org.apache.commons.net.ntp TimeStamp]))

(set! *warn-on-reflection* true)

(defn config-from-map
  "Convert Clojure map to Zenoh Config.
  Accepts:
  {:mode :peer | :client | :router
   :connect {:endpoints [\"tcp/[::]:7447\" ...]}
   :listen {:endpoints [\"tcp/[::]:7447\" ...]}}
  
  Or simplified form:
  {:mode :peer
   :connect [\"tcp/[::]:7447\" ...]
   :listen [\"tcp/[::]:7447\" ...]}"
  [{:keys [connect listen] :as config-map}]
  (cond
    (nil? config-map) (Config/loadDefault)
    (string? config-map) (Config/fromJson5 config-map)
    (map? config-map)
    (let [normalized (cond-> config-map
                       (sequential? connect)
                       (assoc :connect {:endpoints connect})
                       (sequential? listen)
                       (assoc :listen {:endpoints listen}))
          json (json/write-json-str normalized)]
      (Config/fromJson5 json))
    :else (Config/loadDefault)))

(defn open
  "Open a Zenoh session.
  Config can be:
  - nil (uses default config)
  - map with {:mode :peer/:client/:router
              :connect {:endpoints [\"tcp/[::]:7447\"]}
              :listen {:endpoints [\"tcp/[::]:7447\"]}}
  - simplified map {:mode :client :connect [\"tcp/[::]:7447\"]}
  - JSON5 string
  Returns a [[io.zenoh.Session]] object. Must be closed with [[close!]] or use [[with-session]]."
  ([]
   (open nil))
  ([config]
   (let [cfg (config-from-map config)]
     (Zenoh/open cfg))))

(defn close!
  "Close a Zenoh session."
  [^Session session]
  (.close session))

(defn closed?
  "Returns true is Zenoh session is closed."
  [^Session session]
  (.isClosed session))

(defn session-info
  "Get session information including connected peers and routers.
  Returns a map with:
  {:zid <ZenohId>            ; Session's Zenoh ID
   :peers [<ZenohId> ...]    ; Connected peers
   :routers [<ZenohId> ...]} ; Connected routers"
  [^Session session]
  (when (and session (not (closed? session)))
    (let [info (.info session)]
      {:zid (.zid info)
       :peers (vec (.peersZid info))
       :routers (vec (.routersZid info))})))

(defn connected? [session]
  (let [{:keys [peers routers]} (session-info session)]
    (boolean (or (seq peers) (seq routers)))))

(defmacro with-session [[binding config] & body]
  `(with-open [~binding (open ~config)]
     ~@body))

(defn timestamp->date
  "Convert Zenoh TimeStamp to java.util.Date."
  [^TimeStamp ts]
  (when ts (.getDate ts)))

(defn timestamp->instant
  "Convert Zenoh TimeStamp to java.time.Instant."
  [^TimeStamp ts]
  (when ts (Instant/ofEpochMilli (.getTime ts))))

(defn current-timestamp
  "Get current time as Zenoh TimeStamp (an apache commons ntp TimeStamp)."
  []
  (TimeStamp/getCurrentTime))

(defn sample->map
  "Convert [[io.zenoh.sample.Sample]] to a clojure map"
  [^Sample sample]
  (when sample
    {:key-expr (.toString (.getKeyExpr sample))
     :payload (utils/zbytes->str (.getPayload sample))
     :encoding (utils/encoding->str (.getEncoding sample))
     :timestamp (.getTimestamp sample)
     :kind (keyword (str/lower-case (.getKind sample)))
     :attachment (when-let [att (.getAttachment sample)]
                   (utils/zbytes->str att))}))

(defn- ^PublisherOptions publisher-options
  "Build [[io.zenoh.pubsub.PublisherOptions]] from options map.
  Accepts:
  {:encoding :zenoh/string | :application/json | etc
   :congestion-control :drop | :block
   :priority :realtime | :interactive-high | :interactive-low | :data-high | :data | :data-low | :background
   :reliability :reliable | :best-effort}"
  [{:keys [congestion-control encoding reliability] :as opts}]
  (when (seq opts)
    (let [^PublisherOptions po (PublisherOptions.)]
      (when encoding (.setEncoding po (utils/->encoding encoding)))
      (when congestion-control (.setCongestionControl po (utils/->congestion-control congestion-control)))
      (when reliability (.setReliability po (utils/->reliability reliability)))
      po)))

(defn publisher
  "Declare a publisher on a key expression.
  Returns a [[io.zenoh.pubsub.Publisher]] that can be used with `put!`.
  Must be closed with [[close!]] or use [[with-publisher]].
  Options:
  {:encoding :zenoh/string
   :congestion-control :block
   :reliability :reliable}"
  ([^Session session key-expr]
   (publisher session key-expr nil))
  ([^Session session key-expr opts]
   (let [ke (utils/->key-expr key-expr)
         ^PublisherOptions pub-opts (publisher-options opts)]
     (if pub-opts
       (.declarePublisher session ke pub-opts)
       (.declarePublisher session ke)))))

(defn- ^PutOptions put-options
  "Build PutOptions from Clojure map.
  Accepts:
  {:encoding :text/plain | :application/json | etc
   :attachment 'metadata string' | byte-array}"
  [{:keys [encoding attachment] :as opts}]
  (when (seq opts)
    (let [^PutOptions po (PutOptions.)]
      (when encoding (.setEncoding po (utils/->encoding encoding)))
      (when attachment (.setAttachment po ^ZBytes (utils/->zbytes attachment)))
      po)))

(defn- publisher-put! [^Publisher pub data opts]
  (let [^ZBytes zbytes (utils/->zbytes data)
        ^PutOptions put-opts (put-options opts)]
    (if put-opts
      (.put pub zbytes put-opts)
      (.put pub zbytes))))

(defn put!
  "Publish data with a [[io.zenoh.pubsub.Session]]
   or [[io.zenoh.pubsub.Publisher]]."
  ([session-or-publisher data]
   (put! session-or-publisher data nil))
  ([session-or-publisher key-or-data opts-or-data]
   (if (instance? Publisher session-or-publisher)
     (publisher-put! session-or-publisher key-or-data (or opts-or-data {}))
     (let [^Session session session-or-publisher
           key-expr (utils/->key-expr key-or-data)
           data opts-or-data
           ^ZBytes zbytes (utils/->zbytes data)]
       (.put ^Session session ^KeyExpr key-expr ^ZBytes zbytes))))
  ([^Session session key-expression data opts]
   (let [^KeyExpr key-expr (utils/->key-expr key-expression)
         ^ZBytes zbytes (utils/->zbytes data)
         ^PutOptions put-opts (put-options opts)]
     (if put-opts
       (.put ^Session session ^KeyExpr key-expr ^ZBytes zbytes ^PutOptions put-opts)
       (.put ^Session session ^KeyExpr key-expr ^ZBytes zbytes)))))

(defmacro with-publisher
  "Execute body with a publisher in a [[with-open]] try expression."
  [[binding publisher-expr] & body]
  `(with-open [~binding ~publisher-expr]
     ~@body))

(defn handler
  "Create a Zenoh Handler from a function.
  The function f is called with the sample/query/reply.
  Returns a Handler that returns true from receiver()."
  [f]
  (reify Handler
    (handle [_ x] (f x))
    (receiver [_] true)
    (onClose [_] nil)))

(defn on-sample [f]
  (handler (fn [^Sample s] (f (sample->map s)))))

(defn subscriber
  "Declare a subscriber on a key expression with a custom Handler or function.
  Handler must implement [[io.zenoh.handlers.Handler]].
  If a function is provided, it is wrapped with [[on-sample]].
  Returns [[io.zenoh.pubsub.Subscriber]]. Must be closed with .close or use with-open."
  [^Session session key-expression handler-or-fn]
  (let [key-expr (utils/->key-expr key-expression)
        handler (if (fn? handler-or-fn) (on-sample handler-or-fn) handler-or-fn)]
    (.declareSubscriber ^Session session ^KeyExpr key-expr ^Handler handler)))

(defn delete!
  "Delete a key from Zenoh."
  ([^Session session key-expr]
   (let [ke (utils/->key-expr key-expr)]
     (.delete session ke))))

(defn reply->map
  "Convert [[io.zenoh.query.Reply]] to a clojure map"
  [^Reply reply]
  (when (instance? Reply$Success reply)
    (when-let [sample (.getSample ^Reply$Success reply)] 
      (sample->map sample))))

(defn on-reply [f]
  (handler (fn [^Reply r]
             (when (instance? Reply$Success r)
               (when-let [m (reply->map r)]
                 (f m))))))

(defn- ^GetOptions get-options
  "Build [[io.zenoh.query.GetOptions]] from options map.
  Accepts:
  {:payload 'query data' | byte-array
   :encoding :application/json | etc
   :attachment 'metadata'}"
  [{:keys [attachment encoding payload] :as opts}]
  (when (seq opts)
    (let [^GetOptions go (GetOptions.)]
      (when attachment (.setAttachment go ^ZBytes (utils/->zbytes attachment)))
      (when encoding (.setEncoding go (utils/->encoding encoding)))
      (when payload (.setPayload go ^ZBytes (utils/->zbytes payload)))
      go)))

(defn get!
  "Query data from Zenoh with a custom Handler or function.
  The Handler must implement [[io.zenoh.handlers.Handler]] interface.
  If a function is provided, it is wrapped with [[on-reply]].
  Returns the value returned by the Handler's receiver() (true if using function).
  The selector can include parameters: 'key/expr?param1=value1;param2=value2'
  
  Options:
  {:payload 'query data'
   :encoding :application/json
   :attachment 'metadata'}"
  ([^Session session selector handler-or-fn]
   (get! session selector handler-or-fn nil))
  ([^Session session selector handler-or-fn opts]
   (let [sel (utils/->selector selector)
         handler (if (fn? handler-or-fn) (on-reply handler-or-fn) handler-or-fn)
         ^GetOptions get-opts (get-options opts)]
     (if get-opts
       (.get ^Session session ^Selector sel ^Handler handler ^GetOptions get-opts)
       (.get ^Session session ^Selector sel ^Handler handler)))))

(defn query->map
  "Convert [[io.zenoh.query.Query]] to a clojure map"
  [^Query query]
  (when query
    (let [selector (.getSelector query)
          params (.getParameters selector)]
      (with-meta
        {:key-expr (str (.getKeyExpr query))
         :selector (str selector)
         :parameters (when params (.toMap params))
         :payload (when-let [p (.getPayload query)]
                    (utils/zbytes->str p))
         :encoding (when-let [e (.getEncoding query)]
                     (utils/encoding->str e))
         :attachment (when-let [att (.getAttachment query)]
                       (utils/zbytes->str att))}
        {:zenoh/query query}))))

(defn reply!
  "Reply to a query.
  Target can be a [[io.zenoh.query.Query]] object or a map returned by [[query->map]]."
  ([target key-expression data]
   (let [^Query query (if (map? target)
                        (:zenoh/query (meta target))
                        target)]
     (when query
       (let [key-expr (utils/->key-expr key-expression)
             zbytes (utils/->zbytes data)]
         (.reply ^Query query ^KeyExpr key-expr ^ZBytes zbytes))))))


(defn on-query [f]
  (handler (fn [^Query q] (f (query->map q)))))

(defn- ^QueryableOptions queryable-options
  "Build [[io.zenoh.query.QueryableOptions]] from options map.
  Accepts: {:complete true | false}"
  [{:keys [complete] :as opts}]
  (doto (QueryableOptions.)
    (.setComplete (boolean complete))))

(defn queryable
  "Declare a queryable on a key expression with a custom Handler or function.
  Handler must implement [[io.zenoh.handlers.Handler]].
  If a function is provided, it is wrapped with [[on-query]].
  Returns [[io.zenoh.query.Queryable]]. Must be closed with .close or use with-open.
  Options: {:complete true}  ; Signal this queryable provides complete/exhaustive information"
  ([^Session session key-expression handler-or-fn]
   (queryable session key-expression handler-or-fn nil))
  ([^Session session key-expression handler-or-fn opts]
   (let [key-expr (utils/->key-expr key-expression)
         handler (if (fn? handler-or-fn) (on-query handler-or-fn) handler-or-fn)
         ^QueryableOptions queryable-opts (queryable-options opts)]
     (if queryable-opts
       (.declareQueryable ^Session session ^KeyExpr key-expr ^Handler handler ^QueryableOptions queryable-opts)
       (.declareQueryable ^Session session ^KeyExpr key-expr ^Handler handler)))))
