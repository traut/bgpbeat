(ns bgpbeat.tests.transformations
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.java.io :as io])
  (:require [bgpbeat.core :as core]
            [bgpbeat.utils :as utils]
            :reload))


(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (println
      ex "bgpbeat:\nuncaught exception on" (.getName thread)))))


(defn read-message [filename]
  (utils/parse-json (slurp (io/resource filename))))


(deftest flatten-announcements
  (testing "The announcements from the original message should be flattened into one message per announcement"
    (let [full-message (read-message "message-announcements.json")
          message (select-keys full-message [:announcements])]
      (is (= [{:prefix "10.1.1.0/24" :update-type :announcement :next-hop "10.1.0.1"}
              {:prefix "10.1.2.0/24" :update-type :announcement :next-hop "10.1.0.1"}
              {:prefix "10.2.1.0/24" :update-type :announcement :next-hop "10.2.0.1"}
              {:prefix "10.2.2.0/24" :update-type :announcement :next-hop "10.2.0.1"}]
             (core/flatten-announcements message))))
    (is (= nil (core/flatten-announcements {:withdrawals ["10.1.0.0./16"]})))))


(deftest flatten-withdrawals
  (testing "Verify that withdrawals from the original update message are flattened into message per withdrawal"
    (let [full-message (read-message "message-withdrawals.json")
          message (select-keys full-message [:withdrawals])]
      (is (= [{:prefix "10.1.0.0/16" :update-type :withdrawal}
              {:prefix "10.2.0.0/16" :update-type :withdrawal}]
             (core/flatten-withdrawals message))))
    (is (= nil (core/flatten-withdrawals {:announcements []})))))
