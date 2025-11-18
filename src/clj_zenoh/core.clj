(ns clj-zenoh.core
  "Clojure wrapper for Zenoh Java wrapper/API."
  (:require [charred.api :as json]
            [clojure.string :as str])
  (:import [io.zenoh Config Session Zenoh]
           [io.zenoh.bytes Encoding ZBytes]
           [io.zenoh.handlers Callback Handler]
           [io.zenoh.keyexpr KeyExpr]
           [io.zenoh.pubsub Publisher PublisherOptions PutOptions Subscriber]
           [io.zenoh.qos CongestionControl Reliability]
           [io.zenoh.query GetOptions Query Queryable QueryableOptions Reply Reply$Success Selector]
           [io.zenoh.sample Sample]
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

(defn- normalize-key-expr
  "Convert keyword/symbol to string without leading colon.
  Keywords: :test/key -> 'test/key', :key -> 'key'
  Symbols: test/key -> 'test/key'
  Strings: passed through"
  [k]
  (cond
    (or (keyword? k) (symbol? k))
    (if-let [key-namespace (namespace k)]
      (str key-namespace "/" (name k))
      (name k))
    :else (str k)))

(defn- opt->val [x]
  (if (instance? java.util.Optional x)
    (.orElse ^java.util.Optional x nil)
    x))

(defn ^KeyExpr ->key-expr
  "Convert string to [[io.zenoh.KeyExpr]]."
  [key-expr]
  (if (instance? KeyExpr key-expr)
    key-expr
    (let [val (-> (normalize-key-expr key-expr)
                  (KeyExpr/tryFrom)
                  (opt->val))]
      (or val (throw (ex-info "Invalid key expression" {:value key-expr}))))))

(defn- ->zbytes
  "Convert data to [[io.zenoh.bytes.ZBytes]]."
  [data]
  (cond
    (instance? ZBytes data) data
    (string? data) (ZBytes/from ^String data)
    (bytes? data) (ZBytes/from ^bytes data)
    :else (ZBytes/from (str data))))

(defn- zbytes->str
  "Convert [[io.zenoh.bytes.ZBytes]] to a string."
  [^ZBytes zbytes]
  (when zbytes (.toString zbytes)))

(def ^:private encoding-map
  "Map of keywords to [[io.zenoh.bytes.Encoding]] constants"
  {:zenoh/string Encoding/ZENOH_STRING
   :zenoh/bytes Encoding/ZENOH_BYTES
   :text/plain Encoding/TEXT_PLAIN
   :text/json Encoding/TEXT_JSON
   :text/csv Encoding/TEXT_CSV
   :text/html Encoding/TEXT_HTML
   :text/xml Encoding/TEXT_XML
   :application/json Encoding/APPLICATION_JSON
   :application/xml Encoding/APPLICATION_XML
   :application/octet-stream Encoding/APPLICATION_OCTET_STREAM})

(def ^:private encoding-reverse-map
  ;; TODO utils ns + get rid of this def
  (into {} (map (fn [[k v]] [v k]) encoding-map)))

(defn- ->encoding
  "Convert keyword to [[io.zenoh.bytes.Encoding]]."
  [enc]
  (cond
    (instance? Encoding enc) enc
    (keyword? enc) (get encoding-map enc)
    :else nil))

(defn- encoding->str
  "Convert [[io.zenoh.bytes.Encoding]] to string.
  Maps encoding object to name since description is lost across JNI."
  [^Encoding enc]
  (when enc
    (if-let [kw (get encoding-reverse-map enc)]
      (let [ns-part (namespace kw)
            name-part (name kw)]
        (str ns-part "/" name-part))
      (.toString enc))))

(defn- ->enum [^Class enum-cls kw]
  (when kw
    (try
      (Enum/valueOf enum-cls (-> (name kw)
                                 (str/upper-case)
                                 (str/replace "-" "_")))
      (catch IllegalArgumentException _ nil))))

(defn- ->congestion-control
  "Convert keyword to [[io.zenoh.qos.CongestionControl]]."
  [cc]
  (->enum CongestionControl cc))

(defn- ->reliability
  "Convert keyword to [[io.zenoh.qos.Reliability]]."
  [rel]
  (->enum Reliability rel))

(defn sample->map
  "Convert [[io.zenoh.sample.Sample]] to a clojure map"
  [^Sample sample]
  (when sample
    {:key-expr (.toString (.getKeyExpr sample))
     :payload (zbytes->str (.getPayload sample))
     :encoding (encoding->str (.getEncoding sample))
     :timestamp (.getTimestamp sample)
     :kind (keyword (str/lower-case (.getKind sample)))
     :attachment (when-let [att (.getAttachment sample)]
                   (zbytes->str att))}))

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
      (when encoding (.setEncoding po (->encoding encoding)))
      (when congestion-control (.setCongestionControl po (->congestion-control congestion-control)))
      (when reliability (.setReliability po (->reliability reliability)))
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
   (let [ke (->key-expr key-expr)
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
      (when encoding (.setEncoding po (->encoding encoding)))
      (when attachment (.setAttachment po ^ZBytes (->zbytes attachment)))
      po)))

(defn- publisher-put! [^Publisher pub data opts]
  (let [^ZBytes zbytes (->zbytes data)
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
           key-expr (->key-expr key-or-data)
           data opts-or-data
           ^ZBytes zbytes (->zbytes data)]
       (.put session key-expr zbytes))))
  ([^Session session key-expr data opts]
   (let [ke (->key-expr key-expr)
         ^ZBytes zbytes (->zbytes data)
         ^PutOptions put-opts (put-options opts)]
     (if put-opts
       (.put session ke zbytes put-opts)
       (.put session ke zbytes)))))

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
  [^Session session key-expr handler-or-fn]
  (let [ke (->key-expr key-expr)
        h (if (fn? handler-or-fn) (on-sample handler-or-fn) handler-or-fn)]
    (.declareSubscriber session ke ^Handler h)))

(defn delete!
  "Delete a key from Zenoh."
  ([^Session session key-expr]
   (let [ke (->key-expr key-expr)]
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

(defn ^Selector ->selector
  "Convert string to [[io.zenoh.query.Selector]]."
  [selector-str]
  (if (instance? Selector selector-str)
    selector-str
    (let [val (-> (normalize-key-expr selector-str)
                  (Selector/tryFrom)
                  (opt->val))]
      (or val (throw (ex-info "Invalid selector" {:value selector-str}))))))

(defn- ^GetOptions get-options
  "Build [[io.zenoh.query.GetOptions]] from options map.
  Accepts:
  {:payload 'query data' | byte-array
   :encoding :application/json | etc
   :attachment 'metadata'}"
  [{:keys [attachment encoding payload] :as opts}]
  (when (seq opts)
    (let [^GetOptions go (GetOptions.)]
      (when attachment (.setAttachment go ^ZBytes (->zbytes attachment)))
      (when encoding (.setEncoding go (->encoding encoding)))
      (when payload (.setPayload go ^ZBytes (->zbytes payload)))
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
   (let [sel (->selector selector)
         h (if (fn? handler-or-fn) (on-reply handler-or-fn) handler-or-fn)
         ^GetOptions get-opts (get-options opts)]
     (if get-opts
       (.get session sel ^Handler h get-opts)
       (.get session sel ^Handler h)))))

(defn query->map
  "Convert [[io.zenoh.query.Query]] to a clojure map"
  [^Query query]
  (when query
    (let [selector (.getSelector query)
          params (.getParameters selector)]
      {:key-expr (str (.getKeyExpr query))
       :selector (str selector)
       :parameters (when params (.toMap params))
       :payload (when-let [p (.getPayload query)]
                  (zbytes->str p))
       :encoding (when-let [e (.getEncoding query)]
                   (encoding->str e))
       :attachment (when-let [att (.getAttachment query)]
                     (zbytes->str att))})))

(defn on-query [f]
  (handler (fn [^Query q] (f (query->map q)))))

(defn- ^QueryableOptions queryable-options
  "Build [[io.zenoh.query.QueryableOptions]] from options map.
  Accepts: {:complete true | false}"
  [{:keys [complete] :as opts}]
  (when (seq opts)
    (let [^QueryableOptions qo (QueryableOptions.)]
      (when (some? complete) (.setComplete qo (boolean complete)))
      qo)))

(defn queryable
  "Declare a queryable on a key expression with a custom Handler or function.
  Handler must implement [[io.zenoh.handlers.Handler]].
  If a function is provided, it is wrapped with [[on-query]].
  Returns [[io.zenoh.query.Queryable]]. Must be closed with .close or use with-open.
  Options: {:complete true}  ; Signal this queryable provides complete/exhaustive information"
  ([^Session session key-expr handler-or-fn]
   (queryable session key-expr handler-or-fn nil))
  ([^Session session key-expr handler-or-fn opts]
   (let [ke (->key-expr key-expr)
         h (if (fn? handler-or-fn) (on-query handler-or-fn) handler-or-fn)
         ^QueryableOptions q-opts (queryable-options opts)]
     (if q-opts
       (.declareQueryable session ke ^Handler h q-opts)
       (.declareQueryable session ke ^Handler h)))))
