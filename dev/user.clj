(ns user
  (:require [clj-zenoh.core :as z]
            [clojure.core.async :as a]))

(comment
  (def session (z/open {:mode :client 
                        :connect [7447]}))

  (z/session-info session)
  (z/connected? session)
  (z/close! session)
  (.isClosed session)

  (a/go 
    (doseq [i (range 10)]
      (a/<!! (a/timeout (* (inc i) 100)))
      (z/put! session (format "demo/example/v4/test_%d" i)
              {:some {:nested :data :crazy (range (inc i))}})))
  
  (def sub (z/subscriber session "demo/example/**"
                         (reify io.zenoh.handlers.Handler
                           (handle [_ sample]
                             (clojure.pprint/pprint (str "> " sample)))
                           (receiver [_] true)
                           (onClose [_] nil))))

  (def ch (a/chan))
  (z/get! session "demo/example/v1/**"
          (reify io.zenoh.handlers.Handler
            (handle [_ reply]
              (a/put! ch (z/reply->map reply)))
            (receiver [_] ch)
            (onClose [_] nil)))

  (a/close! ch)
  (.close sub)
  (.close session)
  
  (z/with-session [s {:mode :client :connect [7447]}]
    (z/put! s "demo/example/v8" "test-data"))
  
  )
