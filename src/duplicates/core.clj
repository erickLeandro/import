(ns duplicates.core
  (:require [clojure.java.jdbc :as j]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [select from where group having order-by join delete-from left-join]]
            [clojure.java.io :as io]
            [environ.core :refer [env]]))

;; =============================================================================
;; Config vars
;; =============================================================================
(def organograma 3)

;; =============================================================================
;; DB Setup
;; =============================================================================

(def db-target {:subprotocol "mysql"
                :subname "//s2.virtuaserver.com.br:3306/centersi_test?zeroDateTimeBehavior=convertToNull"
                :user "centersi_01"
                :password "J6foEAiP!{1^"})

(defn get-invalid-entries []
  (j/query
   db-target
   (-> (select :u.usuarioCod)
       (from [:_usuario :u])
       (left-join [:pessoa :p] [:= :u.usuarioCod :p.usuarioCod])
       (where [:in :usuarioLogin (-> (select :usuarioLogin)
                                     (from [:_usuario :u2])
                                     (where [:= :u2.organogramaCod organograma])
                                     (group :u2.usuarioLogin)
                                     (having [:> :%count.* 1]))]
              [:= :u.organogramaCod organograma]
              [:= :p.usuarioCod nil])
        (order-by :usuarioLogin)
       (sql/format))))

(defn remove-user [id]
  (println "Removing user login: " id)
    (j/with-db-transaction [conn db-target]
      (j/delete! db-target :_log ["usuarioCod = ?" id])
      (j/delete! db-target :_usuario_recovery ["usuarioCod = ?" id])
      (j/delete! db-target :_usuario ["usuarioCod = ?" id])))

(defn check-invalids! []
  (println "Start check duplicates")
  (let [ids (get-invalid-entries)]
    (doall
     (map (fn [x] (remove-user (:usuariocod x))) ids))))

(defn -main [& args]
  (check-invalids!)
  (println "Done!"))