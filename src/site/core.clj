(ns site.core
  (:require [clojure.java.jdbc :as j]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [select from where group having order-by join delete-from]]
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
             :subname "//127.0.0.1:3306/potencia?zeroDateTimeBehavior=convertToNull"
             :user "root"
             :password "q1w2e3"})

;; =============================================================================
;; File functions
;; =============================================================================

(defn get-invalid-entries []
  (->> (j/query db-target
          (-> (select :usuarioCod)
              (from :_usuario)
              (where [:in :usuarioLogin (->
                                          (select :usuarioLogin)
                                          (from :_usuario)
                                          (where [:= :organogramaCod organograma])
                                          (group :usuarioLogin)
                                          (having [:> :%count.* 1])
                                        )])
              (order-by [:usuarioLogin :numeroAcessos :desc])
              (sql/format)))))



(defn get-entries []
  (->> (j/query db-target
        (-> (select :u.usuarioCod)
            (from [:_usuario :u])
            (join :pessoa [:= :pessoa.usuarioCod :u.usuarioCod])
            (where [:= :u.organogramaCod organograma])
            (sql/format)))))


(defn valids-ids [] (into [] (map (fn [x] (get x :usuariocod)) (get-entries))))

(defn invalids-ids [] (into [] (map (fn [x] (get x :usuariocod)) (get-invalid-entries))))

(defn in?
  "true if coll contains elm"
  [coll elm]
  (some #(= elm %) coll))

(defn remove-user [id]
  (println "Removing user: " id)
  (j/delete! db-target :_usuario ["usuarioCod = ?" id]))

(defn check-invalids []
  (println "Start check duplicates")
  (doall
    (map (fn [x]
      (if (not (in? (valids-ids) x))
        (remove-user x)))
(invalids-ids))))

(defn -main [& args]
  (check-invalids)
  (println "Done!"))

(defn filter-ids [valids invalids])
