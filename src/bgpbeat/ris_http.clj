(ns bgpbeat.ris-http
  (:gen-class)
  (:require [clj-http.client :as http]
            [cheshire.core :as cheshire]

            [bgpbeat.utils :as utils]))

(def ripe-ris-http-uri (format "https://ris-live.ripe.net/v1/stream/?format=json&client=%s" utils/client-name))

(defn read-http-stream []
  (utils/log :info "Starting to read a HTTP stream" :uri ripe-ris-http-uri)
  (let [reader (-> ripe-ris-http-uri
                   (http/get {:as :reader})
                   :body)
        lines (line-seq reader)
        events (map #(cheshire/parse-string % true) lines)]
    events))


(defn keep-only-ris-update-messages [events]
  (->> events
       (filter #(= (:type %) "ris_message"))
       (filter #(= (:type (:data %)) "UPDATE"))))
