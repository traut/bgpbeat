(ns bgpbeat.ris-ws
  (:gen-class)
  (:require [gniazdo.core :as ws]
            [cheshire.core :as cheshire]

            [bgpbeat.utils :as utils]))


(def ripe-ris-ws-uri (format "wss://ris-live.ripe.net/v1/ws/?client=%s" utils/client-name))


(defn read-ws-stream [callback]
  (utils/log :info "Subscribing to a websocket stream" :uri ripe-ris-ws-uri)
  (let [socket (ws/connect
                 ripe-ris-ws-uri
                 :on-receive #(apply callback [(cheshire/parse-string % true)]))
        subscribe-me {:type "ris_subscribe" :data {:type "UPDATE"}}]
    (utils/log :info "Sending subscription message" :msg subscribe-me)
    (ws/send-msg socket (cheshire/generate-string subscribe-me))))
