(ns clojure-course-task03.core
  (:require [clojure.set])
  (:require [clojure.string :refer [lower-case]]))

(defn join* [table-name conds]
  (let [op (first conds)
        f1 (name (nth conds 1))
        f2 (name (nth conds 2))]
    (str table-name " ON " f1 " " op " " f2)))

(defn where* [data]
  (let [ks (keys data)
        res (reduce str (doall (map #(let [src (get data %)
                                           v (if (string? src)
                                               (str "'" src "'")
                                               src)]
                                       (str (name %) " = " v ",")) ks)))]
    (reduce str (butlast res))))

(defn order* [column ord]
  (str (name column)
       (if-not (nil? ord) (str " " (name ord)))))

(defn limit* [v] v)

(defn offset* [v] v)

(defn -fields** [data]
  (reduce str (butlast (reduce str (doall (map #(str (name %) ",") data))))))

(defn fields* [flds allowed]
  (let [v1 (apply hash-set flds)
        v2 (apply hash-set allowed)
        v (clojure.set/intersection v1 v2)]
    (cond
     (and (= (first flds) :all) (= (first allowed) :all)) "*"
     (and (= (first flds) :all) (not= (first allowed) :all)) (-fields** allowed)
     (= :all (first allowed)) (-fields** flds)
     :else (-fields** (filter v flds)))))


(defn select* [table-name {:keys [fields where join order limit offset]}]
  (-> (str "SELECT " fields " FROM " table-name " ")
      (str (if-not (nil? where) (str " WHERE " where)))
      (str (if-not (nil? join) (str " JOIN " join)))
      (str (if-not (nil? order) (str " ORDER BY " order)))
      (str (if-not (nil? limit) (str " LIMIT " limit)))
      (str (if-not (nil? offset) (str " OFFSET " offset)))))


(defmacro select [table-name & data]
  (let [;; Var containing allowed fields
        fields-var# (symbol (str table-name "-fields-var"))

        ;; The function takes one of the definitions like (where ...) or (join ...)
        ;; and returns a map item [:where (where* ...)] or [:join (join* ...)].
        transf (fn [elem]
                 (let [v (first elem)
                       v2 (second elem)
                       v3 (if (> (count elem) 2) (nth elem 2) nil)
                       val (case v
                               fields (list 'fields* (vec (next elem)) fields-var#)
                               offset (list 'offset* v2)
                               limit (list 'limit* v2)
                               order (list 'order* v2 v3)
                               join (list 'join* (list 'quote v2) (list 'quote v3))
                               where (list 'where* v2))]
                   [(keyword v) val]))

        ;; Takes a list of definitions like '((where ...) (join ...) ...) and returns
        ;; a vector [[:where (where* ...)] [:join (join* ...)] ...].
        env* (loop [d data
                    v (first d)
                    res []]
               (if (empty? d)
                 res
                 (recur (next d) (second d) (conj res (transf v)))))

        ;; Accepts vector [[:where (where* ...)] [:join (join* ...)] ...],
        ;; returns map {:where (where* ...), :join (join* ...), ...}
        env# (apply hash-map (apply concat env*))]
    
    `(select* ~(str table-name)  ~env#)))


;; Task implementation
;; ===================

#_(defn resolve [^clojure.lang.Symbol s] (atom (eval s))) ; LightTable workaround

(defn group-symbol [^String group]
   (symbol (str "group-" group)))

(defn resolve-group [group]
  (deref (resolve (group-symbol group))))

(defn fields-var-symbol [^String table]
  (symbol (str table "-fields-var")))

(defn select-symbol [name* ^String table]
  (symbol (str "select-" (lower-case name*) "-" table)))

(defn group-definition [name* table-kw fields]
  (let [table (name table-kw)]
    `(defn ~(select-symbol name* table) []
                  (let [~(fields-var-symbol table) ~fields]
                    (select ~table
                            (~'fields ~@fields))))))
 
(defmacro group [name & body]
  (let [permission-table (->> body 
                         (partition 3)
                         (mapcat (fn [[table _ fields]] [(keyword table) (mapv keyword fields)]))
                         (apply hash-map))
        group-defs (map #(apply (partial group-definition name) %) permission-table)]
    `(do 
       ~@group-defs
       (def ~(group-symbol name) ~permission-table))))
  
(defn compact-fields [x]
  (if (some #{:all} x)
    [:all]
    (vec x)))

(defn merge-tables [^clojure.lang.IPersistentMap x 
                    ^clojure.lang.IPersistentMap y] 
  (merge-with (comp compact-fields concat) x y))

(defn user-permission-tables-symbol [^String name*]
  (symbol (str name* "-permission-tables")))

(defn resolve-user-permission-tables [name*]
  (deref (resolve (user-permission-tables-symbol name*))))

(defmacro user [name* & body]
  (let [in-body (first body)
        belongs-to (rest in-body)]
    (assert (= (first in-body) 'belongs-to) "Syntax error")
    (let [permission-tables 
          (->> belongs-to 
               (map resolve-group) 
               (reduce merge-tables))]
      `(def ~(user-permission-tables-symbol name*) ~permission-tables))))

(defmacro with-user [name* & body]
  (let [bindings (->> name*
                      (resolve-user-permission-tables)
                      (mapcat #(list 
                                (-> % first name fields-var-symbol)
                                (second %)))
                      (vec))]
    `(let ~bindings ~@body)))


(comment
; Usage

(group Agent
       proposal -> [person, phone, address, price]
       agents -> [client_id, proposal_id, agent])

(group Operator
       proposal -> [:all]
       clients -> [:all])

(group Director
       proposal -> [:all]
       clients -> [:all]
       agents -> [:all])

(select-agent-proposal) ;; select person, phone, address, price from proposal;
(select-agent-agents)  ;; select client_id, proposal_id, agent from agents;
group-Agent

(resolve-group "Agent")

(compact-fields [:all])
(compact-fields '(:all :all))
(compact-fields [:a :b :c])
(compact-fields [:a :b :all :c])

(user Directorov
      (belongs-to Operator,
                  Agent,
                  Director))

(resolve-user-permission-tables "Directorov")

(with-user Directorov (select clients (fields :all)))

)


