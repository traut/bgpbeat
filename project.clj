(defproject bgpbeat "0.1.0-SNAPSHOT"
  :description "Bgpbeat streams BGP updates from RIPE NCC RIS project into Elasticsearch instance"
  :url "https://github.com/traut/bgpbeat"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [mvxcvi/puget "1.3.2"]
                 [stylefruits/gniazdo "1.2.0"]
                 [elastic/elasticsearch-clojure "8.0.0"]]
  :main ^:skip-aot bgpbeat.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot [bgpbeat.core]
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
             :test {:resource-paths ["test/resources"]}}
  :plugins [[lein-eftest "0.5.9"]])
