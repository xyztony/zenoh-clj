(ns clj-zenoh.liveliness-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as async :refer [go <! >! chan]]
            [clj-zenoh.core :as z]
            [clj-zenoh.liveliness :as zl]
            [clj-zenoh.test.helpers :as h]))

(use-fixtures :once h/with-shared-session)

(h/deftest-with-timeout liveliness-token-test
  (testing "can declare and query liveliness token"
    (let [session (h/test-session)
          reply-ch (chan)]
      (zl/with-token [token (zl/declare-token session :test/liveliness)]
        (is (some? token))
        (zl/get! session :test/liveliness (h/channel-handler reply-ch z/reply->map))
        (let [reply (h/<!!-with-timeout reply-ch)]
          (is (= "test/liveliness" (:key-expr reply)))))
      (async/close! reply-ch))))

(h/deftest-with-timeout liveliness-subscriber-test
  (testing "subscriber receives token events"
    (let [session (h/test-session)
          sample-ch (chan)]
      (with-open [sub (zl/subscriber
                       session "test/sub-live/**"
                       (h/channel-handler sample-ch z/sample->map))]
        (zl/with-token [token (zl/declare-token session "test/sub-live/1")]
          (let [put-sample (h/<!!-with-timeout sample-ch)]
            (is (= "test/sub-live/1" (:key-expr put-sample)))
            (is (= :put (:kind put-sample)))))
        ;; Token closed (after with-token closed)
        (let [del-sample (h/<!!-with-timeout sample-ch)]
          (is (= "test/sub-live/1" (:key-expr del-sample)))
          (is (= :delete (:kind del-sample)))))
      (async/close! sample-ch))))

(h/deftest-with-timeout liveliness-history-test
  (testing "subscriber with history receives existing tokens"
    (let [session (h/test-session)
          sample-ch (chan)]
      (zl/with-token [token (zl/declare-token session :test/history)]
        (with-open [sub (zl/subscriber
                         session :test/history 
                         (h/channel-handler sample-ch z/sample->map)
                         {:history true})]
          (let [sample (h/<!!-with-timeout sample-ch)]
            (is (= "test/history" (:key-expr sample)))
            (is (= :put (:kind sample))))))
      (async/close! sample-ch))))
