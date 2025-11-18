(ns clj-zenoh.scout-test
  (:require [clojure.test :refer :all]
            [clj-zenoh.core :as z]
            [clj-zenoh.scout :as s]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clj-zenoh.test.helpers :as h])
  (:import [io.zenoh.scouting Scout Hello]
           [io.zenoh.handlers Handler]))

(defn with-discovery-session [f]
  ;; start a session so there is something to find
  (z/with-session [_ {:mode :peer}] (f)))

(use-fixtures :once with-discovery-session)

(h/deftest-with-timeout test-scout-callback
  ;; only interested in the first result to demonstrate discovery works
  (let [found (async/promise-chan)]
    (s/with-scout [scout {:what-am-i #{:peer}} 
                   (fn [hello] (async/put! found hello))]
      (let [result (h/<!!-with-timeout found)]
        (is (not (str/blank? (:zid result))) "ZID should be a non-empty string")
        (is (seq (:locators result)) "Locators should not be empty")
        (is (= :peer (:what-am-i result)))))
    (async/close! found)))

(h/deftest-with-timeout test-scout-blocking
  (s/with-scout [scout {:what-am-i #{:peer}}]
    (let [receiver (.getReceiver scout)
          hello-opt (.poll receiver 5 java.util.concurrent.TimeUnit/SECONDS)]
      (is (and hello-opt (.isPresent hello-opt)))
      (is (= :peer (->> (.get hello-opt)
                        s/hello->map
                        :what-am-i))))))

(h/deftest-with-timeout test-scout-default-what-am-i
  (s/with-scout [scout {}] ; No what-am-i specified
    (let [receiver (.getReceiver scout)
          hello-opt (.poll receiver 1 java.util.concurrent.TimeUnit/SECONDS)]
      (is (and hello-opt (.isPresent hello-opt)))
      (is (#{:peer :router} (:what-am-i (s/hello->map (.get hello-opt))))))))

(h/deftest-with-timeout test-scout-handler
  (let [found (async/promise-chan)
        handler (h/channel-handler found s/hello->map)]
    (s/with-scout [scout {:what-am-i #{:peer}} handler]
      (let [result (h/<!!-with-timeout found)]
        (is (= :peer (:what-am-i result)))))
    (async/close! found)))
