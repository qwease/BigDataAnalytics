(ns cljdetector.storage.storage
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.conversion :refer [from-db-object]]))

(def DEFAULT-DBHOST "localhost")
(def DEFAULT-CANDIDATE-CHUNKSIZE 100000)
(def dbname "cloneDetector")
(def partition-size 100)
(def CANDIDATE-CHUNKSIZE-PARAM  (System/getenv "CANDIDATE-CHUNKSIZE"))
(def CANDIDATE-CHUNKSIZE (if CANDIDATE-CHUNKSIZE-PARAM (Integer/parseInt CANDIDATE-CHUNKSIZE-PARAM) DEFAULT-CANDIDATE-CHUNKSIZE))
(def hostname (or (System/getenv "DBHOST") DEFAULT-DBHOST))
(def collnames ["files" "chunks" "candidates" "clones" "statusUpdates" "statistics" "processCompleted"])

;; MODIFICATION: new function
(defn addUpdate!
  "Add a timestamped message to the statusUpdates collection"
  [message]
  (let [conn (mg/connect {:host hostname})
        db   (mg/get-db conn dbname)]
    (mc/insert db "statusUpdates" {:timestamp (java.time.LocalDateTime/now)
                                   :message   message})))
;; MODIFICATION: new function
(defn mark-process-completed! []
  (let [conn (mg/connect {:host hostname})        
        db (mg/get-db conn dbname)]
    (mc/insert db "processCompleted" {:timestamp (java.time.LocalDateTime/now)})))

(defn print-statistics []
  (let [conn (mg/connect {:host hostname})        
        db (mg/get-db conn dbname)]
    (doseq [coll collnames]
      (println "db contains" (mc/count db coll) coll))))

(defn clear-db! []
  (let [conn (mg/connect {:host hostname})        
        db (mg/get-db conn dbname)]
    (doseq [coll collnames]
      (mc/drop db coll))))

(defn count-items [collname]
  (let [conn (mg/connect {:host hostname})        
        db (mg/get-db conn dbname)]
    (mc/count db collname)))

(defn store-files! [files]
  (let [conn (mg/connect {:host hostname})        
        db (mg/get-db conn dbname)
        collname "files"
        file-parted (partition-all partition-size files)]
    (doseq [file-group file-parted]
           (mc/insert-batch db collname (map (fn [%] {:fileName (.getPath %) :contents (slurp %)}) file-group)))
         ))

(defn store-chunks! [chunks]
  (let [conn (mg/connect {:host hostname})        
        db (mg/get-db conn dbname)
        collname "chunks"
        chunk-parted (partition-all partition-size (flatten chunks))]
    (doseq [chunk-group chunk-parted]
      (mc/insert-batch db collname (map identity chunk-group)))))

(defn store-clones! [clones]
  (let [conn (mg/connect {:host hostname})        
        db (mg/get-db conn dbname)
        collname "clones"
        clones-parted (partition-all partition-size clones)]
    (doseq [clone-group clones-parted]
      (mc/insert-batch db collname (map identity clone-group)))))

;; (defn identify-candidates! []
;;   (let [conn (mg/connect {:host hostname})        
;;         db (mg/get-db conn dbname)
;;         collname "chunks"]
;;      (mc/aggregate db collname
;;                    [{$group {:_id {:chunkHash "$chunkHash"}
;;                              :numberOfInstances {$count {}}
;;                              :instances {$push {:fileName "$fileName"
;;                                                 :startLine "$startLine"
;;                                                 :endLine "$endLine"}}}}
;;                     {$match {:numberOfInstances {$gt 1}}}
;;                     {"$out" "candidates"} ])))

;; MODIFICATION:
(defn identify-candidates!
  "Combine distinct chunkHash collection, partition them, and
   run an aggregation for each chunk, merging into 'candidates'.
   Ensures we have an index on 'chunkHash' to avoid error 51183."
  ([] (identify-candidates! CANDIDATE-CHUNKSIZE))
  ([chunk-size]
   (let [conn (mg/connect {:host hostname})
         db   (mg/get-db conn dbname)
         coll "chunks"]

     ;; 1) Create (or ensure) that an index on chunkHash exists.
     ;;    If you require chunkHash to be globally unique, set :unique true.
     (mc/create-index db "candidates"
                      (array-map :chunkHash 1)
                      {:unique true})
    
     ;; 2) Get distinct chunk hashes via a group pipeline (avoid mc/distinct 16MB limit).
     (let [distinct-pipeline  [{:$group {:_id "$chunkHash"}}]
           distinct-results    (mc/aggregate db coll distinct-pipeline)
           all-chashes        (map :_id distinct-results)
           partitions         (partition-all chunk-size all-chashes)
           total              (count partitions)]

       (println (format "Partitioned into %d chunks (chunk-size = %d)."
                        total chunk-size))

       ;; 3) For each subset of chunk hashes, run the aggregation pipeline.
       (doseq [[idx chunk-group] (map-indexed vector partitions)]
         (let [pipeline
               [{:$match {:chunkHash {:$in chunk-group}}}
                {:$group {:_id "$chunkHash"
                          :numberOfInstances {:$count {}}
                          :instances {:$push {:fileName  "$fileName"
                                              :startLine "$startLine"
                                              :endLine   "$endLine"}}}}
                {:$match {:numberOfInstances {:$gt 1}}}
                {:$set   {:chunkHash "$_id"}}
                {:$unset :_id}
                {:$merge {:into           "candidates"
                          :on             "chunkHash"
                          :whenMatched    "merge"
                          :whenNotMatched "insert"}}]]
           ;; Execute this chunk
           (mc/aggregate db coll pipeline)
           (println (format "Processed subset %d of %d."
                            (inc idx) total))))

       (println "Done. 'candidates' collection updated.")))))
;; MODIFICATION:

(defn consolidate-clones-and-source []
  (let [conn (mg/connect {:host hostname})        
        db (mg/get-db conn dbname)
        collname "clones"]
    (mc/aggregate db collname
                  [{$project {:_id 0 :instances "$instances" :sourcePosition {$first "$instances"}}}
                   {"$addFields" {:cloneLength {"$subtract" ["$sourcePosition.endLine" "$sourcePosition.startLine"]}}}
                   {$lookup
                    {:from "files"
                     :let {:sourceName "$sourcePosition.fileName"
                           :sourceStart {"$subtract" ["$sourcePosition.startLine" 1]}
                           :sourceLength "$cloneLength"}
                     :pipeline
                     [{$match {$expr {$eq ["$fileName" "$$sourceName"]}}}
                      {$project {:contents {"$split" ["$contents" "\n"]}}}
                      {$project {:contents {"$slice" ["$contents" "$$sourceStart" "$$sourceLength"]}}}
                      {$project
                       {:_id 0
                        :contents 
                        {"$reduce"
                         {:input "$contents"
                          :initialValue ""
                          :in {"$concat"
                               ["$$value"
                                {"$cond" [{"$eq" ["$$value", ""]}, "", "\n"]}
                                "$$this"]
                               }}}}}]
                     :as "sourceContents"}}
                   {$project {:_id 0 :instances 1 :contents "$sourceContents.contents"}}])))


(defn get-dbconnection []
  (mg/connect {:host hostname}))

(defn get-one-candidate [conn]
  (let [db (mg/get-db conn dbname)
        collname "candidates"]
    (from-db-object (mc/find-one db collname {}) true)))

(defn get-overlapping-candidates [conn candidate]
  (let [db (mg/get-db conn dbname)
        collname "candidates"
        clj-cand (from-db-object candidate true)]
    (mc/aggregate db collname
                  [{$match {"instances.fileName" {$all (map #(:fileName %) (:instances clj-cand))}}}
                   {$addFields {:candidate candidate}}
                   {$unwind "$instances"}
                   {$project 
                    {:matches
                     {$filter
                      {:input "$candidate.instances"
                       :cond {$and [{$eq ["$$this.fileName" "$instances.fileName"]}
                                    {$or [{$and [{$gt  ["$$this.startLine" "$instances.startLine"]}
                                                 {$lte ["$$this.startLine" "$instances.endLine"]}]}
                                          {$and [{$gt  ["$instances.startLine" "$$this.startLine"]}
                                                 {$lte ["$instances.startLine" "$$this.endLine"]}]}]}]}}}
                     :instances 1
                     :numberOfInstances 1
                     :candidate 1
                     }}
                   {$match {$expr {$gt [{$size "$matches"} 0]}}}
                   {$group {:_id "$_id"
                            :candidate {$first "$candidate"}
                            :numberOfInstances {$max "$numberOfInstances"}
                            :instances {$push "$instances"}}}
                   {$match {$expr {$eq [{$size "$candidate.instances"} "$numberOfInstances"]}}}
                   {$project {:_id 1 :numberOfInstances 1 :instances 1}}])))

(defn remove-overlapping-candidates! [conn candidates]
  (let [db (mg/get-db conn dbname)
        collname "candidates"]
      (mc/remove db collname {:_id {$in (map #(:_id %) candidates)}})))

(defn store-clone! [conn clone]
  (let [db (mg/get-db conn dbname)
        collname "clones"
        anonymous-clone (select-keys clone [:numberOfInstances :instances])]
    (mc/insert db collname anonymous-clone)))
