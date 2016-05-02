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
  (let [id              (:old-id old-news-entry)
        kws             (:file-kws old-news-entry)
        possible-fnames (map (fn [kw]
                               (str news-path "/" (file-id kw) "/" id ".jpg"))
                             kws)
        existing-files  (filter #(.exists %) (map io/file possible-fnames))]
    existing-files))

(defn with-files [entries]
  (map
   (fn [entry]
     (let [valid-kws (valid-files entry)
          files (-> (assoc entry :file-kws valid-kws)
                    (find-in-fs))]
      (-> entry
        (dissoc :old-id 
                :foto1 
                :foto2 
                :foto3 
                :foto4
                :foto5
                :foto6
                :file-kws)
        (assoc :files files))))
   entries))

;; TODO
;; 1) Gerar um map de Noticias Antigas -> Noticias Novas
;; 2) Salvar as noticias, retornando o id do banco novo
;; 3) Mover os arquivos existens para o Storage
;; 4) Criar o registro no banco de dados na tabela _upload

(defn new-entry [{:keys [id titulo texto data foto1 foto2 foto3 foto4 foto5 foto6]}]
  {:noticia_titulo   titulo
   :organogramaCod 5
   :noticia_tipo "P"
   :noticia_conteudo texto
   :noticia_data     data
   :noticia_privacidade "P"
   :noticia_comentario "N"
   :old-id id
   :foto1 foto1
   :foto2 foto2
   :foto3 foto3
   :foto4 foto4
   :foto5 foto5
   :foto6 foto6})

(defn persist-entry [db-target entries]
  (j/insert-multi! db-target :noticia entries))

(->> (j/query
     db-source
     (-> (select :*)
         (from :mac_noticias)
         (where [:= :id 806])
         (sql/format)))
    (map new-entry)
    (with-files))