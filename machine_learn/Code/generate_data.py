import pandas as pd

class EvaluationData:
    
    def __init__(self):
        self.relevance_docs = {}
        self.read_relevance()

        self.query_doc = {}
        self.df = pd.DataFrame()

        self.es_scores = {}
        self.bm_scores = {}
        self.otf_scores = {}
        self.tfidf_scores = {}
        self.lml_scores = {}
        self.lmjm_scores = {}

        self.read_es_scores()
        self.read_bm_scores()
        self.read_otf_scores()
        self.read_tfidf_scores()
        self.read_lml_scores()
        self.read_lmjm_scores()
        self.get_final_query_doc()
        self.construct_data_frame()

    def read_relevance(self): # read and store relevance assessments
        with open("./original_data/qrels.adhoc.51-100.AP89.txt", "r") as f:
            for line in f.readlines():
                line = line.replace("\n", "")
                query_id, temp, doc_id, rel = line.split(" ")
                if query_id in self.relevance_docs:
                    self.relevance_docs[query_id][doc_id] = rel
                else:
                    self.relevance_docs[query_id] = {}
                    self.relevance_docs[query_id][doc_id] = rel

    def read_es_scores(self):
        with open("./original_data/ES_builtin_output.txt", "r") as f:
            for line in f.readlines():
                line = line.replace("\n", "")
                query_id, temp, doc_id, rank, score, exp = line.split(" ")
                if query_id in self.es_scores:
                    self.es_scores[query_id][doc_id] = score
                else:
                    self.es_scores[query_id] = {}
                    self.es_scores[query_id][doc_id] = score

    def read_bm_scores(self):
        with open("./original_data/Okapi_BM25_output.txt", "r") as f:
            for line in f.readlines():
                line = line.replace("\n", "")
                query_id, temp, doc_id, rank, score, exp = line.split(" ")
                if query_id in self.bm_scores:
                    self.bm_scores[query_id][doc_id] = score
                else:
                    self.bm_scores[query_id] = {}
                    self.bm_scores[query_id][doc_id] = score

    def read_otf_scores(self):
        with open("./original_data/Okapi_TF_output.txt", "r") as f:
            for line in f.readlines():
                line = line.replace("\n", "")
                query_id, temp, doc_id, rank, score, exp = line.split(" ")
                if query_id in self.otf_scores:
                    self.otf_scores[query_id][doc_id] = score
                else:
                    self.otf_scores[query_id] = {}
                    self.otf_scores[query_id][doc_id] = score

    def read_tfidf_scores(self):
        with open("./original_data/TF_IDF_output.txt", "r") as f:
            for line in f.readlines():
                line = line.replace("\n", "")
                query_id, temp, doc_id, rank, score, exp = line.split(" ")
                if query_id in self.tfidf_scores:
                    self.tfidf_scores[query_id][doc_id] = score
                else:
                    self.tfidf_scores[query_id] = {}
                    self.tfidf_scores[query_id][doc_id] = score

    def read_lml_scores(self):
        with open("./original_data/LM_Jelinek_Mercer_output.txt", "r") as f:
            for line in f.readlines():
                line = line.replace("\n", "")
                query_id, temp, doc_id, rank, score, exp = line.split(" ")
                if query_id in self.lml_scores:
                    self.lml_scores[query_id][doc_id] = score
                else:
                    self.lml_scores[query_id] = {}
                    self.lml_scores[query_id][doc_id] = score

    def read_lmjm_scores(self):
        with open("./original_data/LM_Laplace_output.txt", "r") as f:
            for line in f.readlines():
                line = line.replace("\n", "")
                query_id, temp, doc_id, rank, score, exp = line.split(" ")
                if query_id in self.lmjm_scores:
                    self.lmjm_scores[query_id][doc_id] = score
                else:
                    self.lmjm_scores[query_id] = {}
                    self.lmjm_scores[query_id][doc_id] = score

    def get_final_query_doc(self):
        for i in self.es_scores:
            self.query_doc[i] = set()
            for doc in self.relevance_docs[i]:
                self.query_doc[i].add(doc)
            for doc in self.es_scores[i]:
                self.query_doc[i].add(doc)


    def construct_data_frame(self):
        # Initialize lists for DataFrame construction
        queries = []
        docs = []
        features = {
            "es": [],
            "otf": [],
            "bm": [],
            "tfidf": [],
            "lml": [],
            "lmjm": []
        }
        rel = []

        # Populate lists from stored scores
        for q in self.query_doc:
            for doc in self.query_doc[q]:
                queries.append(q)
                docs.append(doc)
                features["es"].append(self.es_scores.get(q, {}).get(doc, 0))
                features["otf"].append(self.otf_scores.get(q, {}).get(doc, 0))
                features["bm"].append(self.bm_scores.get(q, {}).get(doc, 0))
                features["tfidf"].append(self.tfidf_scores.get(q, {}).get(doc, 0))
                features["lml"].append(self.lml_scores.get(q, {}).get(doc, 0))
                features["lmjm"].append(self.lmjm_scores.get(q, {}).get(doc, 0))
                rel.append(self.relevance_docs.get(q, {}).get(doc, 0))

        # Create DataFrame
        self.df = pd.DataFrame({
            "query": queries,
            "doc": docs,
            "es": features["es"],
            "otf": features["otf"],
            "bm": features["bm"],
            "tfidf": features["tfidf"],
            "lml": features["lml"],
            "lmjm": features["lmjm"],
            "label": rel
        })

# data_handler = EvaluationData()
# print(data_handler.df)
    
