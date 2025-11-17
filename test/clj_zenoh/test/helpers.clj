
(ns clj-zenoh.test.helpers
  (:require [clojure.test :refer [deftest use-fixtures]]
            [clj-zenoh.core :as z]
            [clojure.core.async :as async]))

(def ^:dynamic *test-timeout-ms* 5000)
(defonce ^:private session* (atom nil))
(defn test-session [] @session*)

(defn with-shared-session [f]
  (z/with-session [s {:mode :peer
                      :connect []
                      :listen []
                      :scouting {:multicast {:enabled false}
                                 :gossip {:enabled false}}}]
    (reset! session* s)
    (try (f) (finally (reset! session* nil)))))

(defn <!!-with-timeout
  ([ch] (<!!-with-timeout ch *test-timeout-ms*))
  ([ch timeout-ms]
   (let [[v port] (async/alts!! [ch (async/timeout timeout-ms)])]
     (when (= port ch) v))))

(defmacro deftest-with-timeout
  "Macro over [[deftest]] with configurable timeout."
  [name & body]
  (let [[opts body] (if (map? (first body))
                      [(first body) (rest body)]
                      [{} body])
        timeout-ms (get opts :timeout *test-timeout-ms*)]
    `(deftest ~name
       (binding [*test-timeout-ms* ~timeout-ms]
         ~@body))))

(defn channel-handler
  "Create a Handler that puts items to a channel and closes it when done.
  Takes a channel and a converter function (e.g., z/sample->map or z/reply->map).
  Returns a Handler implementation that:
  - Converts each received item using the converter function
  - Puts the converted item into the channel
  - Returns the channel from receiver()
  - Closes the channel on onClose()"
  [ch converter-fn]
  (reify io.zenoh.handlers.Handler
    (handle [_ item] (async/put! ch (converter-fn item)))
    (receiver [_] ch)
    (onClose [_] (async/close! ch))))

(defn returning-handler
  "Create a Handler that processes items with a function but returns a specific value.
  Takes a handler function and a return value for receiver().
  Returns a Handler implementation that:
  - Calls handler-fn for each received item
  - Returns return-val from receiver()
  - Does nothing on onClose() (unless on-close-fn provided)
  Useful for queryables where you process queries but don't need to return a collection."
  ([handler-fn return-val]
   (returning-handler handler-fn return-val (constantly nil)))
  ([handler-fn return-val on-close-fn]
   (reify io.zenoh.handlers.Handler
     (handle [_ item] (handler-fn item))
     (receiver [_] return-val)
     (onClose [_] (on-close-fn)))))
