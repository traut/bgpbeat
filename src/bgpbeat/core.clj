(ns bgpbeat.core
  (:gen-class)
  (:require [clj-http.client :as http]
            [gniazdo.core :as ws]
            [clojure.string :as s]
            [elasticsearch.document :as doc]
            [elasticsearch.connection.http :as conn]
            [cheshire.core :as cheshire]
            [clojure.java.io :as io]
            [puget.printer :as puget]
            [clojure.core.async :as async]))

(def client-name "bgpbeat")

(def ripe-ris-http-uri (format "https://ris-live.ripe.net/v1/stream/?format=json&client=%s" client-name))
(def ripe-ris-ws-uri (format "wss://ris-live.ripe.net/v1/ws/?client=%s" client-name))

(def es-url (System/getenv "ES_URL"))
(def es-index (System/getenv "ES_INDEX"))
(def es-username (System/getenv "ES_AUTH_USERNAME"))
(def es-password (System/getenv "ES_AUTH_PASSWORD"))

(def logging-level :info)

(def messages-chan (async/chan (async/sliding-buffer 5000)))
(def batches-chan (async/chan (async/sliding-buffer 100)))

(def messages-chan-max-time-secs 60)

(def es-submitter-workers-num (let [env-val (System/getenv "ES_WORKERS_NUM")]
                                (if (string? env-val) (Integer/parseInt env-val) 5)))

(def es-batch-size 1000)


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


(defn set-at-timestamp-field [message]
  (-> message
    (assoc "@timestamp" (long (* (:timestamp message) 1000)))
    (dissoc :timestamp)))


(defn set-path-str [message]
  (if (:path message)
    (assoc message :path-str (s/join " " (:path message)))
    message))


(defn remove-raw [message]
  (dissoc message :raw))


(defn unpack-communities [message]
  (let [community-pairs (:community message)
        communities (mapv (fn [[asn value]] {:asn asn :value value})
                          community-pairs)]
    (assoc message :community communities)))


(defn keep-only-ris-update-messages [events]
  (->> events
       (filter #(= (:type %) "ris_message"))
       (filter #(= (:type (:data %)) "UPDATE"))))


(defn read-ris-http-stream []
  (log :info "Starting to read a HTTP stream" :uri ripe-ris-http-uri)
  (let [reader (-> ripe-ris-http-uri
                   (http/get {:as :stream})
                   :body
                   io/reader)
        ; :reader can be used with chechire > 5.9.0, but ES client uses 5.7.1
        ; client (http/get ripe-ris-http-uri {:as :reader})
        lines (line-seq reader)
        events (map #(cheshire/parse-string % true) lines)]
    events))


(defn read-ris-ws-stream [callback]
  (log :info "Subscribing to a websocket stream" :uri ripe-ris-ws-uri)
  (let [socket (ws/connect
                 ripe-ris-ws-uri
                 :on-receive #(apply callback [(cheshire/parse-string % true)]))
        subscribe-me {:type "ris_subscribe" :data {:type "UPDATE"}}]
    (log :info "Sending subscription message" :msg subscribe-me)
    (ws/send-msg socket (cheshire/generate-string subscribe-me))))


(defn apply-to-every-nth [stream func & {:keys [step] :or {step 5000}}]
  (map-indexed (fn [i element]
                 (when (zero? (mod (inc i) step))
                   (apply func [i element]))
                 element)
               stream))


(defn log-stream-progress [stream & {:keys [step] :or {step 5000}}]
  (apply-to-every-nth
    stream
    (fn [i _]
      (log :info "Processing element" :count (inc i)))
    :step step))


(defn as-bulk-action [message]
  {:create {:source message}})


(defn index-batch [client messages]
  (let [start (System/currentTimeMillis)
        actions (mapv as-bulk-action messages)]
    (try
      (doc/bulk client es-index {:body actions})
      (let [end (System/currentTimeMillis)
            n (count actions)
            ms (- end start)
            rate (if (pos? ms) (int (/ n (/ ms 1000))) 0)]
        (log :info "Bulk submitted" :count n :ms ms :rate rate))
      (catch Exception e
        (log :error "Exception during bulk indexing")
        (.printStackTrace e)))))


(defn create-es-client []
  (log :info "Connecting to ES instance" :url es-url :index es-index :username es-username)
  (conn/make {:url es-url
              :basic-auth (if (and es-username es-password)
                            (str es-username ":" es-password)
                            nil)}))


(defn index-message-stream [stream & {:keys [batch-size] :or {batch-size 500}}]
  (let [client (create-es-client)
        do-bulk (fn [messages] (index-batch client messages))]
    (->> stream
         (partition-all batch-size)
         (map do-bulk)
         (doall))))


(defn message-log-trace [message]
  (log :trace "Processing message" :msg message)
  message)


(defn batch [chan-in chan-out max-time-msec batch-size]
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


(defn transform-message [message]
  (try
    (-> message
        :data
        set-at-timestamp-field
        set-path-str
        remove-raw
        unpack-communities)
    (catch Exception e
      (log :error "Exception while transforming the message"
           :msg message
           :error (.getMessage e))
      message)))


(defn consume-messages-from-ws [& {:keys [max-time-secs batch-size]
                                   :or {max-time-secs messages-chan-max-time-secs
                                        batch-size es-batch-size}}]
  (let [client (create-es-client)
        counter (atom 0)]
    (batch messages-chan batches-chan (* max-time-secs 1000) batch-size)
    (read-ris-ws-stream #(async/>!! messages-chan %))

    (let [process-batch (fn [messages-batch]
                          (let [messages (mapv transform-message messages-batch)]
                            (index-batch client messages)
                            (swap! counter + (count messages))
                            (when (zero? (mod @counter 1000))
                              (log :info "Elements processed"
                                   :count @counter
                                   :messages-chan-size (.count (.buf messages-chan))
                                   :batches-chan-size (.count (.buf batches-chan))))))]
      (dotimes [submitter es-submitter-workers-num]
        (log :info (format "Startign an ES bulk submitter %s" submitter))
        (async/go-loop
          []
          (process-batch (async/<! batches-chan))
          (recur)))
      )))


(defn consume-messages-from-http []
  (->> (read-ris-http-stream)
       (keep-only-ris-update-messages)
       (map transform-message)
       (log-stream-progress)
       (index-message-stream)))


(defn -main []
  (when (or (nil? es-url) (nil? es-index))
    (throw (AssertionError. "Either ES URL or index name are not set")))

  ; (consume-messages-from-http)
  (consume-messages-from-ws))

