(ns jepsen.etcdemo
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [slingshot.slingshot :refer [try+]]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [nemesis :as nemesis]
                    [cli :as cli]
                    [tests :as tests]
                    [control :as c]
                    [client :as client]
                    [independent :as independent]
                    [generator :as gen]
                    [db :as db]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 2380))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (name node) "=" (peer-url node))))
       (str/join ",")))

(def dir "/opt/etcd")
(def binary (str dir "/etcd"))
(def logfile (str dir "/etcd.log"))
(def pidfile (str dir "/etcd.pid"))

(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing etcd" version)
      (c/su
        (let [url (str "https://storage.googleapis.com/etcd/" version
                       "/etcd-" version "-linux-amd64.tar.gz")]
          (cu/install-tarball! c/*host* url dir))
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir dir}
          binary
          :--log-output                   :stderr
          :--name                         (name node)
          :--listen-peer-urls             (peer-url   node)
          :--listen-client-urls           (client-url node)
          :--advertise-client-urls        (client-url node)
          :--initial-cluster-state        :new
          :--initial-advertise-peer-urls  (peer-url node)
          :--initial-cluster              (initial-cluster test))

        (Thread/sleep 2000)
        )
      )

    (teardown! [_ test node]
      (info node "tearing down etcd")
      (cu/stop-daemon! binary pidfile)
      (c/su
        (c/exec :rm :-rf dir))
      )

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defn client
  [conn]
  (reify client/Client
    ;; create a new client connected to the specified node
    (setup! [_ test node]
      (client (v/connect (client-url node)
                         {:timeout 5000})))

    (invoke! [_ test op]
      (let [[k v] (:value op)]
        (try+
          (case (:f op)
            :read (let [value (-> conn
                                  (v/get k {:quorum? false})
                                  parse-long)]
                    (assoc op :type :ok, :value (independent/tuple k value)))

            :write (do (v/reset! conn k v)
                       (assoc op :type, :ok))

            :cas (let [[value value'] v]
                   (assoc op :type (if (v/cas! conn k value value'
                                               {:prev-exist? true})
                                     :ok
                                     :fail))))

          (catch java.net.SocketTimeoutException e
            (assoc op
                   :type (if (= :read (:f op)) :fail :info)
                   :error :timeout))

          (catch [:errorCode 100] e
            (assoc op :type :fail, :error :not-found)))))

    (teardown! [_ test]
      )))

(defn etcd-test
  [opts]
  (merge tests/noop-test
         {:name "etcd"
          :db (db "v3.1.5")
          :client (client nil)
          :nemesis (nemesis/partition-random-halves)
          :generator (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w cas])
                                   (gen/stagger 1/10)
                                   (gen/limit 100))))
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))
         :checker (checker/compose
                    {:perf   (checker/perf)
                     :timeline (independent/checker (timeline/html))
                     :linear (independent/checker checker/linearizable)})
         :model (model/cas-register)
         }
         opts))

(defn -main [& args]
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test})
                   (cli/serve-cmd))
            args))

