(ns bgpbeat.utils
  (:gen-class)
  (:require [clojure.core.async :as async]
            [clojure.string :as s]
            [puget.printer :as puget]))

(def logging-level :info)

(def client-name "bgpbeat")


(defn apply-to-every-nth [stream func & {:keys [step] :or {step 5000}}]
  (map-indexed (fn [i element]
                 (when (zero? (mod (inc i) step))
                   (apply func [i element]))
                 element)
               stream))


(defn chan-size [chan]
  (.count (.buf chan)))


(defn batch
  "Fetch messages from `chan-in` channel, collect them in batches
  of size `batch-size` or smaller if `max-time-msec` milliseconds passed,
  and push into `chan-out` channel."
  [chan-in chan-out max-time-msec batch-size]
  (let [counter (dec batch-size)]
    (async/go-loop
      [buffer []
       timer (async/timeout max-time-msec)]
      (let [[v p] (async/alts! [chan-in timer])]
        (cond
          (= p timer)
          (do
            (async/>! chan-out buffer)
            (recur [] (async/timeout max-time-msec)))

          (nil? v)
          (when (seq buffer)
            (async/>! chan-out buffer))

          (== (count buffer) counter)
          (do
            (async/>! chan-out (conj buffer v))
            (recur [] (async/timeout max-time-msec)))

          :else
          (recur (conj buffer v) timer))))))


(defn log [level msg & {:as opts}]
  (let [levels [:trace :debug :info :warning :error]]
    (when (>= (.indexOf levels level)
              (.indexOf levels logging-level))
      (println 
        (str (java.time.LocalDateTime/now))
        (s/upper-case (name level))
        msg
        (if opts
          (puget/cprint-str opts)
          "")))))


(defn read-int-env-var [env-var-name default-value]
  (let [env-val (System/getenv env-var-name)]
    (if (string? env-val)
      (Integer/parseInt env-val)
      default-value)))


(defn log-stream-progress [stream & {:keys [step] :or {step 5000}}]
  (apply-to-every-nth
    stream
    (fn [i _] (log :info "Processing element" :count (inc i)))
    :step step))
