(ns bits-and-bobs.jetlang
  (:import [org.jetlang.channels Channel MemoryChannel LastSubscriber]
           [org.jetlang.core Callback]
           [org.jetlang.fibers Fiber ThreadFiber]
           [java.util.concurrent TimeUnit]
           [student MainJava]))

(defn create-and-start-fiber []
  (doto (ThreadFiber.) (.start)))

(defn schedule [fiber f
                & {:keys [delay timeunit] :or {delay 0 timeunit TimeUnit/MILLISECONDS}}]
  (.schedule fiber f delay timeunit))

(defn schedule-with-fixed-delay [fiber delay f
                                 & {:keys [initial-delay timeunit]
                                    :or {initial-delay 0 timeunit TimeUnit/MILLISECONDS}}]
  (.scheduleWithFixedDelay fiber f initial-delay delay TimeUnit/MILLISECONDS))

(defn create-channel []
  (MemoryChannel.))

(defn callback [f]
  (reify Callback (onMessage [_ msg] (f msg))))

(defn sub [channel fiber f]
  (.subscribe channel fiber (callback f)))

(defn sub-last [channel fiber f
                & {:keys [delay timeunit] :or {delay 0 timeunit TimeUnit/MILLISECONDS}}]
  (.subscribe channel (LastSubscriber. fiber (callback f) delay timeunit)))

(defn pub [channel msg]
  (.publish channel msg))

(let [fiber (create-and-start-fiber) channel (create-channel)]
  (sub channel fiber (fn [msg] (println (str "Every subscriber - " msg))))
  (sub-last channel fiber (fn [msg] (println (str "Last subcriber, zero delay - " msg))))
  (sub-last channel fiber (fn [msg] (println (str "Last subcriber, 250 ms delay - " msg))) :delay 250)

  (dotimes [n 50] (pub channel (str "starting up " n)))
  (schedule-with-fixed-delay fiber 100 (fn [] (pub channel "Periodic message")))
  (schedule fiber (fn [] (pub channel "** Delayed message **")) :delay 1 :timeunit TimeUnit/SECONDS)

  (Thread/sleep 2000))
