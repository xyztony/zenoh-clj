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
