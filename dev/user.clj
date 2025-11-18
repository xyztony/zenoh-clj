(ns user
  (:require [clj-zenoh.core :as z]
            [clojure.core.async :as a]))

(comment
  ;; remember to connect (-e/--connect) routers/peers when running zenohd
  ;; see: https://zenoh.io/docs/getting-started/deployment/#zenoh-router
  (def session (z/open {:mode :peer
                        #_#_:connect ["tcp/localhost:7447"
                                  "tcp/192.168.50.106:7447"]}))

  (def session (z/open {:mode :client 
                        :connect {:endpoints ["tcp/192.168.50.106:7447"]}}))

  (def session
    (z/open
     "{mode: \"client\", connect: {endpoints: [\"tcp/192.168.50.106:7447\"]}}"))
  
  (z/session-info session)
  (z/connected? session)
  (z/close! session)
  (.isClosed session)

  (a/go
    (doseq [i (range 1000)]
      ;; (a/<!! (a/timeout (* (inc i) 100)))
      (z/put! session (format "demo/example/v1/test_%d" i)
              {:some {:nested :data :crazy (range (inc i))}})))
  
  (def v2-reply-chan (a/chan))
  (with-open [queryable (z/queryable
                         session :demo/example/v2
                         (z/on-query
                          (fn [query-map]
                            (a/put! v2-reply-chan query-map))))]
    (z/get! session :demo/example/v2 (z/on-reply (fn [r] (a/put! v2-reply-chan r)))))

  (a/poll! v2-reply-chan)
  
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

  (z/put! session "demo/example/v8" "test-data")
  
  (z/with-session [s {:mode :client :connect ["tcp/localhost:7447"]}]
    (z/put! s "demo/example/v8" "test-data"))

  (def service-chan (a/chan (a/sliding-buffer 100)))

  (def dumb-adder-service-thing
    (z/queryable session "service/math/**"
                 (z/on-query
                  (fn [query]
                    (a/put! service-chan query)))))

  (a/go-loop []
    (when-let [q (a/<! service-chan)]
      (let [sel (:selector q)
            params (:parameters q)
            op (last (clojure.string/split (:key-expr q) #"/"))]
        (println "Service received request:" sel "Params:" params)
        (try
          (case op
            "square"
            (let [x (Double/parseDouble (get params "x" "0"))
                  res (* x x)]
              (z/reply! q (:key-expr q) (str res)))
            
            "add"
            (let [a (Double/parseDouble (get params "a" "0"))
                  b (Double/parseDouble (get params "b" "0"))
                  res (+ a b)]
              (z/reply! q (:key-expr q) (str res)))
            
            (z/reply! q (:key-expr q) (str "Error: Unknown operation " op)))
          (catch Exception e
            (z/reply! q (:key-expr q) (str "Error: " (.getMessage e))))))
      (recur)))

  (doseq [i (range 1000000)]
    (z/get! session
            (format "service/math/%s?x=%d" (if (even? i) "add" "square") i)
            (fn [reply] (println "Result:" (:payload reply)))))

  (z/get! session "service/math/add?a=10;b=32"
          (fn [reply]
            (println "Add Result:" (:payload reply))))

  (.close dumb-adder-service-thing)
  (a/close! service-chan)
  (a/close! results-ch)
  
  )
