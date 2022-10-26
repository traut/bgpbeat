(ns bgpbeat.core
  (:gen-class)
  (:require [clojure.string :as s]
            [clojure.core.async :as async]
            [clojure.java.io :as io]

            ; for streaming messages into a file
            [cheshire.core :as cheshire]

            [bgpbeat.utils :as utils]
            [bgpbeat.ris-http :as ris-http]
            [bgpbeat.ris-ws :as ris-ws]
            [bgpbeat.es :as es]))


(def es-url (System/getenv "ES_URL"))
(def es-index (System/getenv "ES_INDEX"))
(def es-username (System/getenv "ES_AUTH_USERNAME"))
(def es-password (System/getenv "ES_AUTH_PASSWORD"))
(def es-submitter-workers-num (utils/read-int-env-var "ES_WORKERS_NUM" 10))

(def raw-messages-chan (async/chan (async/sliding-buffer 2000)))
(def ready-messages-chan (async/chan (async/sliding-buffer 2000)))
(def flatten-messages-chan (async/chan (async/sliding-buffer 10000)))
(def batches-chan (async/chan (async/sliding-buffer 500)))

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


(defn unpack-communities [message]
  (let [community-pairs (:community message)
        communities (mapv (fn [[asn value]] {:asn asn :value value})
                          community-pairs)]
    (assoc message :community communities)))


(defn normalize-message
  "Normalize raw RIS message into frendlier format"
  [message]
  (try
    (-> message
        (dissoc :raw)  ; remove "raw" field that we have no use for
        set-at-timestamp-field
        set-path-str
        unpack-communities)
    (catch Exception e
      (utils/log :error "Exception while normalizing the message"
           :msg message
           :error (.getMessage e))
      message)))


(defn flatten-announcements [message]
  (let [announcements (:announcements message)]
    (when announcements
      (apply concat (mapv (fn [announcement]
                            (mapv (fn [prefix]
                                    (-> message
                                        (assoc :prefix prefix
                                               :update-type :announcement
                                               ; FIXME: sometimes contains comma-separated IPv6 values
                                               ; :next-hop (:next_hop announcement)
                                               )
                                        (dissoc :announcements)))
                                  (:prefixes announcement)))
                          announcements)))))


(defn flatten-withdrawals [message]
  (let [withdrawals (:withdrawals message)]
    (when withdrawals
      (mapv #(-> message
                 (assoc :prefix %
                        :update-type :withdrawal)
                 (dissoc :withdrawals))
            withdrawals))))


(defn flatten-update-message [message]
  (concat (flatten-announcements message)
          (flatten-withdrawals message)))


(defn flatten-messages-connector [chan-in chan-out]
  (async/go-loop
    []
    (let [message (async/<! chan-in)]
      (mapv #(async/>!! chan-out %) (flatten-update-message message)))
    (recur)))


(defn is-valid [message]
  (= (:type message) "UPDATE"))


(defn dump-messages-connector [chan-in output-file-path & {:keys [limit] :or {limit 10}}]
  (async/go
    (with-open [w (io/writer output-file-path :append false)]
      (loop
        [counter 0]
        (when-let [message (async/<! chan-in)]
          (.write w (cheshire/generate-string message))
          (.newLine w)
          (utils/log :debug "Message dumped to file" :file output-file-path :counter counter))
        (if (< counter limit)
          (recur (inc counter))
          (utils/log :info "Reached the limit" :limit limit))))))


(defn normalize-messages-connector [chan-in chan-out]
  (async/go-loop
    []
    (let [message (async/<! chan-in)]
      (when (is-valid message)
        (async/>!! chan-out (normalize-message message))))
    (recur)))


(defn consume-messages-from-ws
  "Read messages from RIS WebSocket stream and bulk-index them into ES index"
  [& {:keys [max-time-secs batch-size]
      :or {max-time-secs messages-chan-max-time-secs
           batch-size es-batch-size}}]

  (let [es-client (es/create-es-client es-url es-username es-password)]

    ; (dump-messages-connector raw-messages-chan "./messages.jsonl" :limit 1000)

    ; flatten BGP update messages into one message per announcement / withdrawal
    (normalize-messages-connector raw-messages-chan ready-messages-chan)

    ; flatten BGP update messages into one message per announcement / withdrawal
    (flatten-messages-connector ready-messages-chan flatten-messages-chan)

    ; pack messages from `flatten-messages-chan` into batches in `batches-chan`
    (utils/batch flatten-messages-chan
                 batches-chan
                 max-time-secs
                 batch-size)

    ; pack batches from `batches-chan` into actions and submit them to ES
    (es/submit-batches batches-chan
                       es-client
                       es-index
                       es-submitter-workers-num
                       element-processed-log-step))

    ; open the flood gates
    ; (ris-ws/read-ws-stream #(async/>!! raw-messages-chan (:data %))))
    (utils/stream-from-file "./hijack-type-0.jsonl" #(async/>!! raw-messages-chan %)))
    ; (utils/stream-from-file "./hijack-type-0.jsonl" #(utils/log :info "Got message" :message %)))


(defn consume-messages-from-http
  "Read messages from RIS HTTP stream and bulk-index them into ES index"
  []
  (let [es-client (es/create-es-client es-url es-username es-password)]
    (->> (ris-http/read-http-stream)
         (ris-http/keep-only-ris-update-messages)
         (map normalize-message)
         (utils/log-stream-progress)
         (es/index-message-stream es-client es-index))))


(defn -main []
  (when (or (nil? es-url) (nil? es-index))
    (throw (AssertionError. "Either ES URL or index name are not set")))
  ; (consume-messages-from-http)
  (consume-messages-from-ws))

