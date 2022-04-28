(ns bgpbeat.es
  (:gen-class)
  (:require [elasticsearch.document :as doc]
            [elasticsearch.connection.http :as conn]
            [bgpbeat.utils :as utils]))


(defn as-bulk-action [message]
  {:create {:source message}})


(defn create-es-client [es-url es-username es-password]
  (utils/log :info "Connecting to ES instance"
             :url es-url
             :username es-username)
  (conn/make {:url es-url
              :basic-auth (if (and es-username es-password)
                            (str es-username ":" es-password)
                            nil)}))


(defn index-batch [es-client es-index messages]
  (let [start (System/currentTimeMillis)
        actions (mapv as-bulk-action messages)]
    (try
      (doc/bulk es-client es-index {:body actions})
      (let [end (System/currentTimeMillis)
            n (count actions)
            ms (- end start)
            rate (if (pos? ms) (int (/ n (/ ms 1000))) 0)]
        (utils/log :info "Bulk submitted" :count n :ms ms :rate rate))
      (catch Exception e
        (utils/log :error "Exception during bulk indexing")
        (.printStackTrace e)))))


(defn index-message-stream [es-client es-index stream & {:keys [batch-size] :or {batch-size 500}}]
  (let [do-bulk (fn [messages] (index-batch es-client es-index messages))]
    (->> stream
         (partition-all batch-size)
         (map do-bulk)
         (doall))))

