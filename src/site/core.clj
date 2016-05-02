(ns site.core
  (:require [clojure.java.jdbc :as j]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [select join from where limit]]
            [clojure.java.io :as io]
            [environ.core :refer [env]]))

;; =============================================================================
;; Config vars
;; =============================================================================
(def news-path (env :news-path))

;; =============================================================================
;; DB Setup
;; =============================================================================
(def db-source (read-string (env :source-db)))

(def db-target (read-string (env :target-db)))

;; =============================================================================
;; File functions
;; =============================================================================
(defn- valid-files [old-news]
  (let [keywords (map #(keyword (str "foto" %)) (range 1 7))]
    (filter #(not (empty? (get old-news %)))
            keywords)))

(defn- file-id [kw]
  (->> kw
       (str)
       (re-find #"\d")))

(defn- find-in-fs [old-news-entry]
  (let [id              (:id old-news-entry)
        kws             (:file-kws old-news-entry)
        possible-fnames (map (fn [kw]
                               (str news-path "/" (file-id kw) "/" id ".jpg"))
                             kws)
        existing-files  (filter #(.exists %) (map io/file possible-fnames))]
    existing-files))

(defn with-files [old-news]
  (map
   (fn [old-news-entry]
     (let [valid-kws (valid-files old-news-entry)]
       (-> (assoc old-news-entry :file-kws valid-kws)
           (find-in-fs))))
   old-news))

#_(-> (j/query
     db-source
     (-> (select :*)
         (from :mac_noticias)
         (where [:= :id 806])
         (sql/format)))
    (with-files))

;; TODO
;; 1) Gerar um map de Noticias Antigas -> Noticias Novas
;; 2) Salvar as noticias, retornando o id do banco novo
;; 3) Mover os arquivos existens para o Storage
;; 4) Criar o registro no banco de dados na tabela _upload
