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
(def es-submitter-workers-num (utils/read-int-env-var "ES_WORKERS_NUM" 10))

(def messages-chan (async/chan (async/sliding-buffer 2000)))
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


(defn transform-message
  "Transform raw RIS message into the format suitable for indexing"
  [message]
  (try
    (-> message
        :data
        set-at-timestamp-field
        set-path-str
        (dissoc :raw)  ; remove "raw" field that we have no use for
        unpack-communities)
    (catch Exception e
      (utils/log :error "Exception while transforming the message"
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
                                               :next-hop (:next_hop announcement))
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
    (mapv #(async/>!! chan-out %)
          (flatten-update-message (async/<! chan-in)))
    (recur)))


(defn consume-messages-from-ws
  "Read messages from RIS WebSocket stream and bulk-index them into ES index"
  [& {:keys [max-time-secs batch-size]
      :or {max-time-secs messages-chan-max-time-secs
           batch-size es-batch-size}}]

  (let [es-client (es/create-es-client es-url es-username es-password)
        max-time-msecs (* max-time-secs 1000)]

    ; flatten BGP update messages into one message per announcement / withdrawal
    (flatten-messages-connector messages-chan flatten-messages-chan)

    ; pack messages from `flatten-messages-chan` into batches in `batches-chan`
    (utils/batch flatten-messages-chan
                 batches-chan
                 max-time-msecs
                 batch-size)

    ; pack batches from `batches-chan` into actions and submit them to ES
    (es/submit-batches batches-chan
                       es-client
                       es-index
                       es-submitter-workers-num
                       element-processed-log-step))

    ; open the flood gates
    (ris-ws/read-ws-stream #(async/>!! messages-chan (transform-message %))))


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

