(ns clj-zenoh.core
  "Clojure wrapper for Zenoh Java wrapper/API."
  (:require [charred.api :as json]
            [clojure.string :as str])
  (:import [io.zenoh Config Session Zenoh]
           [io.zenoh.bytes Encoding ZBytes]
           [io.zenoh.handlers Callback Handler]
           [io.zenoh.keyexpr KeyExpr]
           [io.zenoh.pubsub Publisher PublisherOptions PutOptions Subscriber]
           [io.zenoh.query Reply Reply$Success]
           [io.zenoh.sample Sample]
           [org.apache.commons.net.ntp TimeStamp]))

(defn- config-from-map
  "Convert Clojure map to Zenoh Config.
  
  Accepts:
    {:mode 'peer' | 'client' | 'router'
     :connect [7447 ...]
     :listen [7447 ...]}"
  [config-map]
  (cond
    (nil? config-map) (Config/loadDefault)
    (string? config-map) (Config/fromJson5 config-map)
    (map? config-map)
    (let [json (json/write-json-str config-map)]
      (Config/fromJson5 json))
    :else (Config/loadDefault)))

(defn open
  "Open a Zenoh session.
  
  Config can be:
    - nil (uses default config)
    - map with {:mode :peer/:client/:router, :connect [...], :listen [...]}
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
    {:zid <ZenohId>           ; Session's Zenoh ID
     :peers [<ZenohId> ...]   ; Connected peers
     :routers [<ZenohId> ...]} ; Connected routers"
  [^Session session]
  (when (and session (not (closed? session)))
    (let [info (.info session)]
      {:zid (.zid info)
       :peers (vec (.peersZid info))
       :routers (vec (.routersZid info))})))

(defn connected? [session]
  (let [{:keys [peers routers]} (session-info session)]
    (or (seq peers) (seq routers))))

(defmacro with-session [[binding config] & body]
  `(with-open [~binding (open ~config)]
     ~@body))

(defn ->key-expr
  "Convert string to [[io.zenoh.KeyExpr]]."
  [key-expr]
  (if (instance? KeyExpr key-expr)
    key-expr
    (KeyExpr/tryFrom (str key-expr))))

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

(defn- publisher-options
  "Build [[io.zenoh.pubsub.PublisherOptions]] from options map.
  
  Accepts:
    {:encoding :zenoh/string | :application/json | etc
     :congestion-control :drop | :block
     :priority :realtime | :interactive-high | :interactive-low | :data-high | :data | :data-low | :background
     :reliability :reliable | :best-effort}"
  [opts]
  (let [pub-opts (PublisherOptions.)]
    (when-let [encoding (:encoding opts)]
      ;; TODO: map keywords to Encoding constants
      )
    (when-let [cc (:congestion-control opts)]
      ;; TODO: map to CongestionControl enum
      )
    (when-let [reliability (:reliability opts)]
      ;; TODO: map to Reliability enum
      )
    pub-opts))

(defn publisher
  "Declare a publisher on a key expression.
  
  Returns a [[io.zenoh.pubsub.Publisher]] that can be used with `put!` multiple times.
  Must be closed with [[close!]] or use [[with-publisher]].
  
  Options:
    {:encoding :zenoh/string
     :congestion-control :block
     :reliability :reliable}
  
  Examples:
    (publisher session 'demo/topic')
    (publisher session 'demo/topic' {:reliability :reliable})"
  ([^Session session key-expr]
   (publisher session key-expr nil))
  ([^Session session key-expr opts]
   (let [ke (->key-expr key-expr)
         pub-opts (publisher-options opts)]
     (.declarePublisher session ke pub-opts))))

(defn- put-options
  "Build PutOptions from Clojure map.
  
  Accepts:
    {:encoding :text/plain | :application/json | etc
     :attachment 'metadata string' | byte-array}"
  [opts]
  (when (seq opts)
    (let [put-opts (PutOptions.)]
      (when-let [enc (:encoding opts)]
        (.setEncoding put-opts (->encoding enc)))
      (when-let [att (:attachment opts)]
        (.setAttachment put-opts (->zbytes att)))
      put-opts)))

(defn- publisher-put! [^Publisher pub data opts]
  (let [zbytes (->zbytes data)
        put-opts (put-options opts)]
    (if put-opts
      (.put pub zbytes put-opts)
      (.put pub zbytes))))

(defn put!
  "Publish data with a [[io.zenoh.pubsub.Session]]
   or [[io.zenoh.pubsub.Publisher]].
  
  Examples:
    (put! session 'demo/key' 'value')
    (put! publisher 'value')
    (put! publisher 'value' {:attachment 'metadata'})"
  ([session-or-publisher data]
   (put! session-or-publisher data nil))
  ([session-or-publisher key-or-data opts-or-data]
   (if (instance? Publisher session-or-publisher)
     (publisher-put! session-or-publisher key-or-data (or opts-or-data {}))
     
     (let [^Session session session-or-publisher
           key-expr (->key-expr key-or-data)
           data opts-or-data
           zbytes (->zbytes data)]
       (.put session key-expr zbytes))))
  ([^Session session key-expr data opts]
   (let [ke (->key-expr key-expr)
         zbytes (->zbytes data)]
     (.put session ke zbytes))))

(defmacro with-publisher
  "Execute body with a publisher in a [[with-open]] try expression.
  
  Example:
    (with-publisher [pub (publisher session 'demo/topic')]
      (put! pub 'message 1')
      (put! pub 'message 2'))"
  [[binding publisher-expr] & body]
  `(with-open [~binding ~publisher-expr]
     ~@body))

(defn subscriber
  "Declare a subscriber on a key expression with a custom Handler.
  
  The Handler must implement [[io.zenoh.handlers.Handler]] interface.
  
  Returns [[io.zenoh.pubsub.Subscriber]]. Must be closed with .close or use with-open.

  Example:
    (let [samples (atom [])]
      (subscriber session 'demo/**'
        (reify Handler
          (handle [_ sample]
            (swap! samples conj (sample->map sample)))
          (receiver [_] samples)
          (onClose [_] nil))))"
  [^Session session key-expr ^Handler handler]
  (let [ke (->key-expr key-expr)]
    (.declareSubscriber session ke handler)))

(defn delete!
  "Delete a key from Zenoh."
  ([^Session session key-expr]
   (delete! session key-expr nil))
  ([^Session session key-expr opts]
   (let [ke (->key-expr key-expr)]
     (.delete session ke))))


(defn reply->map
  "Convert [[io.zenoh.query.Reply]] to a clojure map"
  [^Reply reply]
  (when (instance? Reply$Success reply)
    (when-let [sample (.getSample reply)] 
      (sample->map sample))))

(defn get!
  "Query data from Zenoh with a custom Handler.
  
  The Handler must implement [[io.zenoh.handlers.Handler]] interface.
  
  Returns whatever the Handler's receiver() method returns.
  
  Example:
    (let [replies (atom [])]
      (get! session 'demo/**'
        (reify Handler
          (handle [_ reply]
            (when-let [reply-map (reply->map reply)]
              (swap! replies conj reply-map)))
          (receiver [_] replies)
          (onClose [_] nil)))
      @replies)"
  [^Session session key-expr ^Handler handler]
  (let [ke (->key-expr key-expr)]
    (.get session ke handler)))
