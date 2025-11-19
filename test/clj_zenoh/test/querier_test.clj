(ns clj-zenoh.test.querier-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [clj-zenoh.core :as z]
            [clj-zenoh.querier :as zq]
            [clj-zenoh.test.helpers :as h])
  (:import [java.time Duration]))

(use-fixtures :once h/with-shared-session)

(h/deftest-with-timeout test-querier-basic
  (testing "can make a single querier request"
    (let [calls (atom 0)
          session (h/test-session)]
      (with-open [_ (z/queryable session "test/q"
                                 (fn [query]
                                   (swap! calls inc)
                                   (z/reply! query "test/q" "response")))]
        (zq/with-querier [q (zq/querier session "test/q" {:target :best-matching})]
          (let [ch (async/chan)
                handler (fn [reply] (async/put! ch reply))]
            (zq/get! q handler {:payload "request"})
            (let [reply (h/<!!-with-timeout ch)]
              (is (= "response" (:payload reply))))
            (async/close! ch)
            (is (= 1 @calls))))))))

(h/deftest-with-timeout test-querier-params
  (let [session (h/test-session)]
    (with-open [_ (z/queryable session "test/params"
                               (fn [query]
                                 (let [params (:parameters query)]
                                   (z/reply! query "test/params" 
                                             (str "p1=" (get params "p1"))))))]
      (zq/with-querier [q (zq/querier session "test/params" {:target :best-matching})]
        (let [ch (async/chan)
              handler (fn [reply] (async/put! ch reply))]
          (zq/get! q handler {:parameters {"p1" "v1"}})
          (let [reply (h/<!!-with-timeout ch)]
            (is (= "p1=v1" (:payload reply))))
          (async/close! ch))))))
