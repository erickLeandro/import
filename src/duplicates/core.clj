(ns duplicates.core
  (:require [clojure.java.jdbc :as j]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [select from where group having order-by join delete-from]]
            [clojure.java.io :as io]
            [environ.core :refer [env]]))

;; =============================================================================
;; Config vars
;; =============================================================================
(def organograma 4)

;; =============================================================================
;; DB Setup
;; =============================================================================

(def db-target {:subprotocol "mysql"
                :subname "//localhost:3306/potencia?zeroDateTimeBehavior=convertToNull"
                :user "root"
                :password "q1w2e3"})

(defn get-invalid-entries []
  (j/query
   db-target
   (-> (select :usuarioCod :usuarioLogin :numeroAcessos)
       (from :_usuario)
       (where [:in :usuarioLogin (-> (select :usuarioLogin)
                                     (from :_usuario)
                                     (where [:= :organogramaCod organograma])
                                     (group :usuarioLogin)
                                     (having [:> :%count.* 1]))]
              [:= :organogramaCod organograma])
       (sql/format))))

(defn separate-by-login [rs]
  (reduce (fn [separated user]
            (update-in separated [(:usuariologin user)] #(conj % user)))
          {}
          rs))

(defn duplicates [separated]
  (reduce (fn [to-exclude [_ users]]
            (let [ordered (sort (fn [a b]
                                  (cond
                                    (> (:numeroacessos a) (:numeroacessos b)) -1
                                    (< (:numeroacessos a) (:numeroacessos b)) 1
                                    :else 0))
                                users)]
              (if (seq (rest ordered))
                (conj to-exclude (first (rest ordered)))
                to-exclude)))
          []
          separated))

(defn remove-user [user]
  (println "Removing user: " user)
  (let [id (:usuariocod user)]
    (j/with-db-transaction [conn db-target]
      (j/delete! db-target :_log ["usuarioCod = ?" id])
      (j/delete! db-target :_usuario_recovery ["usuarioCod = ?" id])
      (j/delete! db-target :_usuario ["usuarioCod = ?" id]))))

(defn check-invalids []
  (println "Start check duplicates")
  (let [dups (-> (get-invalid-entries)
                 (separate-by-login)
                 (duplicates))]
    (doall
     (map remove-user dups))))

(defn -main [& args]
  (check-invalids)
  (println "Done!"))
