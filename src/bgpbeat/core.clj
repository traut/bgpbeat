(ns bgpbeat.core
  (:gen-class)
  (:require [clojure.string :as s]
            [clojure.core.async :as async]

            [bgpbeat.utils :as utils]
            [bgpbeat.ris-http :as ris-http]
            [bgpbeat.ris-ws :as ris-ws]
            [bgpbeat.es :as es]))


(def es-url (System/getenv "ES_URL"))
(def es-index (System/getenv "ES_INDEX"))
(def es-username (System/getenv "ES_AUTH_USERNAME"))
(def es-password (System/getenv "ES_AUTH_PASSWORD"))
(def es-submitter-workers-num (utils/read-int-env-var "ES_WORKERS_NUM" 5))

(def messages-chan (async/chan (async/sliding-buffer 5000)))
(def batches-chan (async/chan (async/sliding-buffer 100)))

(def messages-chan-max-time-secs 60)

(def es-batch-size 1000)
(def element-processed-log-step (* es-batch-size 10))


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


(defn message-log-trace [message]
  (utils/log :trace "Processing message" :msg message)
  message)


(defn transform-message
  "Transform raw RIS message into the format suitable for indexing"
  [message]
  (try
    (-> message
        :data
        set-at-timestamp-field
        set-path-str
        remove-raw
        unpack-communities)
    (catch Exception e
      (utils/log :error "Exception while transforming the message"
           :msg message
           :error (.getMessage e))
      message)))


(defn consume-messages-from-ws
  "Read messages from RIS WebSocket stream and bulk-index them into ES index"
  [& {:keys [max-time-secs batch-size]
      :or {max-time-secs messages-chan-max-time-secs
           batch-size es-batch-size}}]
  (let [es-client (es/create-es-client es-url es-username es-password)
        counter (atom 0)
        max-time-msecs (* max-time-secs 1000)]

    (utils/batch messages-chan batches-chan max-time-msecs batch-size)
    (ris-ws/read-ws-stream #(async/>!! messages-chan %))
    (let [process-batch (fn [messages-batch]
                          (let [messages (mapv transform-message messages-batch)]
                            (es/index-batch es-client es-index messages)
                            (swap! counter + (count messages))
                            (when (zero? (mod @counter element-processed-log-step))
                              (utils/log :info "Elements processed"
                                         :count @counter
                                         :messages-chan-size (utils/chan-size messages-chan)
                                         :batches-chan-size (utils/chan-size batches-chan)))))]
      (dotimes [submitter es-submitter-workers-num]
        (utils/log :info (format "Start ES bulk submitter %s" submitter))
        (async/go-loop
          []
          (process-batch (async/<! batches-chan))
          (recur))))))


(defn consume-messages-from-http
  "Read messages from RIS HTTP stream and bulk-index them into ES index"
  []
  (let [es-client (es/create-es-client es-url es-username es-password)]
    (->> (ris-http/read-http-stream)
         (ris-http/keep-only-ris-update-messages)
         (map transform-message)
         (utils/log-stream-progress)
         (es/index-message-stream es-client es-index))))


(defn -main []
  (when (or (nil? es-url) (nil? es-index))
    (throw (AssertionError. "Either ES URL or index name are not set")))
  ; (consume-messages-from-http)
  (consume-messages-from-ws))

