(ns clj-zenoh.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as async :refer [go <! >! chan]]
            [clj-zenoh.core :as z]
            [clj-zenoh.test.helpers :as h]))

(defn- channel->sample-handler [ch]
  (reify io.zenoh.handlers.Handler
    (handle [_ sample]
      (async/put! ch (z/sample->map sample)))
    (receiver [_] ch)
    (onClose [_] (async/close! ch))))

(defn- channel->reply-handler [ch]
  (reify io.zenoh.handlers.Handler
    (handle [_ reply]
      (async/put! ch (z/reply->map reply)))
    (receiver [_] ch)
    (onClose [_] (async/close! ch))))

(use-fixtures :once h/with-shared-session)

(deftest session-lifecycle-test
  (testing "can open and close session"
    (let [session (z/open {:mode :peer
                           :connect []
                           :listen []
                           :scouting {:multicast {:enabled false}
                                      :gossip {:enabled false}}})]
      (is (some? session))
      (is (not (z/closed? session)))
      (z/close! session)
      (is (z/closed? session))))
  (testing "can use with-session"
    (z/with-session [session {:mode :peer
                              :connect []
                              :listen []
                              :scouting {:multicast {:enabled false}
                                         :gossip {:enabled false}}}]
      (is (some? session)))))

(deftest publisher-put-test
  (testing "can create publisher and put data"
    (let [session (h/test-session)]
      (z/with-publisher [pub (z/publisher session :test)]
        (is (instance? io.zenoh.pubsub.Publisher pub))
        (z/put! pub "test message")))))

(h/deftest-with-timeout publisher-put-options-test
  (testing "publisher put with encoding option"
    (let [session (h/test-session)
          ch (chan)]
      (with-open [sub (z/subscriber session :test/encoding
                                    (channel->sample-handler ch))]
        (z/with-publisher [pub (z/publisher session :test/encoding)]
          (z/put! pub "json data" {:encoding :application/json})
          (let [sample (h/<!!-with-timeout ch)]
            (is (= "json data" (:payload sample)))
            (is (= "application/json" (:encoding sample))))))))

  (testing "publisher put with attachment option"
    (let [session (h/test-session)
          ch (chan)]
      (with-open [sub (z/subscriber session :test/attachment
                                    (channel->sample-handler ch))]
        (z/with-publisher [pub (z/publisher session :test/attachment)]
          (z/put! pub "payload" {:attachment "metadata"})
          (let [sample (h/<!!-with-timeout ch)]
            (is (= "payload" (:payload sample)))
            (is (= "metadata" (:attachment sample))))))))

  (testing "publisher put with both encoding and attachment"
    (let [session (h/test-session)
          ch (chan)]
      (with-open [sub (z/subscriber session :test/both
                                    (channel->sample-handler ch))]
        (z/with-publisher [pub (z/publisher session :test/both)]
          (z/put! pub "csv data" {:encoding :text/csv
                                  :attachment "row:123"})
          (let [sample (h/<!!-with-timeout ch)]
            (is (= "csv data" (:payload sample)))
            (is (= "text/csv" (:encoding sample)))
            (is (= "row:123" (:attachment sample)))))))))

(h/deftest-with-timeout subscriber-callback-test
  (testing "subscriber receives published data via callback"
    (let [session (h/test-session)
          [msg1 msg2] ["message 1" "message 2"]
          ch (chan)]
      (with-open [sub (z/subscriber
                       session :test/callback-test (channel->sample-handler ch))]
        (z/put! session :test/callback-test msg1)
        (z/put! session :test/callback-test msg2)
        (let [m1 (:payload (h/<!!-with-timeout ch))
              m2 (:payload (h/<!!-with-timeout ch))]
          (is (= [msg1 msg2] [m1 m2])))))))

(h/deftest-with-timeout subscriber-channel-test
  (testing "subscriber provides core.async channel"
    (let [session (h/test-session)
          [msg1 msg2] ["message 1" "message 2"]
          ch (chan)
          result-ch (chan)]
      (with-open [sub (z/subscriber
                       session :test/channel-test (channel->sample-handler ch))]
        (async/go
          (let [a (:payload (<! ch))
                b (:payload (<! ch))]
            (>! result-ch [a b])))
        (z/put! session :test/channel-test msg1)
        (z/put! session :test/channel-test msg2)
        (is (= [msg1 msg2] (h/<!!-with-timeout result-ch)))
        #_(async/close! (.getReceiver sub))
        (async/close! result-ch)))))

(h/deftest-with-timeout delete-test
  (testing "can delete key"
    (let [session (h/test-session)
          sample-ch (chan)
          reply-ch (chan)]
      (with-open [sub (z/subscriber
                       session :test/to-delete
                       (channel->sample-handler sample-ch))]
        (z/put! session :test/to-delete "will be deleted")
        (z/delete! session :test/to-delete)
        (let [e1 (h/<!!-with-timeout sample-ch)
              e2 (h/<!!-with-timeout sample-ch)]
          (is (= "will be deleted" (:payload e1)))
          (is (= :put (:kind e1)))
          (is (= :delete (:kind e2))))
        (z/get! session :test/to-delete (channel->reply-handler reply-ch))
        (is (nil? (h/<!!-with-timeout reply-ch)))
        (async/close! reply-ch)
        (async/close! (.getReceiver sub))
        (.close sub)))))
