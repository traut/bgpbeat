(ns bgpbeat.utils
  (:gen-class)
  (:require [clojure.core.async :as async]
            [clojure.string :as s]
            [clojure.java.io :as io]

            [cheshire.core :as cheshire]
            [puget.printer :as puget]
            ))

(def logging-level :debug)

(def client-name "bgpbeat")


(defn apply-to-every-nth [stream func & {:keys [step] :or {step 5000}}]
  (map-indexed (fn [i element]
                 (when (zero? (mod (inc i) step))
                   (apply func [i element]))
                 element)
               stream))


(defn chan-size [chan]
  (.count (.buf chan)))


(defn parse-json [value]
  (cheshire/parse-string value true))


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


(defn batch
  "Fetch messages from `chan-in` channel, collect them in batches
  of size `batch-size` or smaller if `max-time-msec` milliseconds passed,
  and push into `chan-out` channel."
  [chan-in chan-out max-time-secs batch-size]
  (let [limit-1 (dec batch-size)
        max-time-msecs (* max-time-secs 1000)]
    (async/go-loop
      [buffer []
       timer (async/timeout max-time-msecs)]
      (let [[v p] (async/alts! [chan-in timer])]
        (cond
          ; no new messages in the channel but timer channel returns
          (= p timer)
          (do
            (if (seq buffer)
              (async/>! chan-out buffer)
              (log :debug "Empty buffer when a timeout reached" :max-time-secs max-time-secs))
            (recur [] (async/timeout max-time-msecs)))

          ; input channel is closed, submit the stuff
          (nil? v)
          (when (seq buffer)
            (async/>! chan-out buffer))

          ; buffer size reaches the max limit
          (>= (count buffer) limit-1)
          (do
            (async/>! chan-out (conj buffer v))
            (recur [] (async/timeout max-time-msecs)))

          :else (do
                  (log :debug "Adding message to buffer" :buffer (count buffer))
                  (recur (conj buffer v) timer)))))))


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


(defn stream-from-file [file-path callback]
  (with-open [reader (io/reader file-path)]
    (doseq [line (line-seq reader)]
      (apply callback [(parse-json line)])))
  (log :info "Streaming from file is done" :file file-path))
