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
  (->> (j/query db-target
          (-> (select :usuarioCod :usuarioLogin :numeroAcessos)
              (from :_usuario)
              (where [:in :usuarioLogin (->
                                          (select :usuarioLogin)
                                          (from :_usuario)
                                          (where [:= :organogramaCod organograma])
                                          (group :usuarioLogin)
                                          (having [:> :%count.* 1])
                                        )] :organogramaCod organograma)
              (sql/format)))))

(defn get-entries []
  (->> (j/query db-target
        (-> (select :u.usuarioCod)
            (from [:_usuario :u])
            (join :pessoa [:= :pessoa.usuarioCod :u.usuarioCod])
            (where [:= :u.organogramaCod organograma])
            (sql/format)))))


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
                (conj to-exclude (rest ordered))
                to-exclude)))
          []
          separated))

(defn valids-ids [] (into [] (map (fn [x] (get x :usuariocod)) (get-entries))))

(defn invalids-ids [] (into [] (map (fn [x] (get x :usuariocod)) (get-invalid-entries))))

(defn in?
  "true if coll contains elm"
  [coll elm]
  (some #(= elm %) coll))

(defn remove-user [id]
  (println "Removing user: " id)
  (j/with-db-transaction [conn db-target]
    (j/delete! db-target :_log ["usuarioCod = ?" id])
    (j/delete! db-target :_usuario_recovery ["usuarioCod = ?" id])
    (j/delete! db-target :_usuario ["usuarioCod = ?" id])))

(defn check-relation-person [id]
  (println "Check relation person" id)
  (empty?
    (->> (j/query db-target
          (-> (select :usuarioCod)
              (from :pessoa)
              (where [:= :usuarioCod id])
              (sql/format))))))

(defn check-invalids []
  (println "Start check duplicates")
  (doall
    (map (fn [x]
      (if (not (check-relation-person x))
        (remove-user x))
      (valids-ids)))))

(defn -main [& args]
  (check-invalids)
  (println "Done!"))

(-> (get-invalid-entries)
    (separate-by-login)
    (duplicates))