(ns clj-zenoh.utils
  "Internal utilities for clj-zenoh."
  (:require [clojure.string :as str])
  (:import [io.zenoh.keyexpr KeyExpr]
           [io.zenoh.bytes Encoding ZBytes]
           [io.zenoh.qos CongestionControl Reliability Priority]
           [io.zenoh.query Selector QueryTarget ConsolidationMode]))

(set! *warn-on-reflection* true)

(defn normalize-key-expr
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

(defn opt->val [x]
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

(defn ->zbytes
  "Convert data to [[io.zenoh.bytes.ZBytes]]."
  [data]
  (cond
    (instance? ZBytes data) data
    (string? data) (ZBytes/from ^String data)
    (bytes? data) (ZBytes/from ^bytes data)
    :else (ZBytes/from (str data))))

(defn zbytes->str
  "Convert [[io.zenoh.bytes.ZBytes]] to a string."
  [^ZBytes zbytes]
  (when zbytes (.toString zbytes)))

(def encoding-map
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

(def encoding-reverse-map
  (into {} (map (fn [[k v]] [v k]) encoding-map)))

(defn ->encoding
  "Convert keyword to [[io.zenoh.bytes.Encoding]]."
  [enc]
  (cond
    (instance? Encoding enc) enc
    (keyword? enc) (get encoding-map enc)
    :else nil))

(defn encoding->str
  "Convert [[io.zenoh.bytes.Encoding]] to string.
  Maps encoding object to name since description is lost across JNI."
  [^Encoding enc]
  (when enc
    (if-let [kw (get encoding-reverse-map enc)]
      (let [ns-part (namespace kw)
            name-part (name kw)]
        (str ns-part "/" name-part))
      (.toString enc))))

(defn ->enum [^Class enum-cls kw]
  (when kw
    (try
      (Enum/valueOf enum-cls (-> (name kw)
                                 (str/upper-case)
                                 (str/replace "-" "_")))
      (catch IllegalArgumentException _ nil))))

(defn ->congestion-control
  "Convert keyword to [[io.zenoh.qos.CongestionControl]]."
  [cc]
  (->enum CongestionControl cc))

(defn ->reliability
  "Convert keyword to [[io.zenoh.qos.Reliability]]."
  [rel]
  (->enum Reliability rel))

(defn ->priority
  "Convert keyword to [[io.zenoh.qos.Priority]]."
  [prio]
  (->enum Priority prio))

(defn ->query-target
  "Convert keyword to [[io.zenoh.query.QueryTarget]]."
  [target]
  (->enum QueryTarget target))

(defn ->consolidation-mode
  "Convert keyword to [[io.zenoh.query.ConsolidationMode]]."
  [mode]
  (->enum ConsolidationMode mode))

(defn ^Selector ->selector
  "Convert string to [[io.zenoh.query.Selector]]."
  [selector-str]
  (if (instance? Selector selector-str)
    selector-str
    (let [val (-> (normalize-key-expr selector-str)
                  (Selector/tryFrom)
                  (opt->val))]
      (or val (throw (ex-info "Invalid selector" {:value selector-str}))))))
