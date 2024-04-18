from collections import defaultdict
from indexing import Indexer
import math

class RetrievalModels:
    def __init__(self, indexer):
        self.indexer = indexer
        
        self.k1 = 1.5
        self.k2 = 100
        self.b = 0.75
        self.N = len(indexer.doc_len_map) # Total number of documents
        self.avgdl = 254.9379 # Average document length
        self.df_map = indexer.df_map # Document frequency map
        self.id_to_name = indexer.id_to_docno
        
        
        self.vocab_size = 198403
        #self.vocab_size = len(indexer.tokenizing.termID_map)  # Vocabulary size
        
        
        
    
    def bm25(self, term_id, doc_id, doc_length):
        term_id = str(term_id)
        tf = self.indexer.get_term_frequency(term_id, doc_id)
        #print(f"tf: {tf}")
        df = self.df_map.get(term_id, 0)
        
        #print(f"df: {df}")
        idf = math.log((self.N + 0.5) / (df + 0.5))
        #print(f"idf: {idf}")
        score = idf * (tf * (self.k1 + 1)) / (tf + self.k1 * (1 - self.b + self.b * (doc_length / self.avgdl))) * ((tf * (self.k2 + 1))/(tf + self.k2) )
        return score

    def tf_idf(self, term_id, doc_id, doc_length):
        term_id = str(term_id)
        
        tf = self.indexer.get_term_frequency(term_id, doc_id)
        df = self.df_map.get(term_id, 0)
        okapi = self.calc_okapi_tf(doc_length, tf, self.avgdl)
        idf = math.log(self.N / (df))
        score = okapi * idf
        return score
    
    def calc_okapi_tf(self, doc_length, tf, avg_len):
        return tf / (tf + 0.5 + 1.5 * (doc_length / avg_len))
        

    
    def lm_laplace(self, term_id, doc_id, doc_length):
        term_id = str(term_id)
        
        tf = self.indexer.get_term_frequency(term_id, doc_id)
        score = math.log((tf + 1) / (doc_length + self.vocab_size))
        return score

    def write_results(self, query_number, scores, model_name):
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        output_file = f"{model_name}_results.txt"
        lines_written = 0
        # print(self.id_to_name)
        with open(output_file, 'a') as file:
            for rank, (doc_id, score) in enumerate(sorted_scores, 1):
                #docno = self.indexer.doc_ids.get(doc_id, "UNKNOWN_DOCNO")
                docno = self.id_to_name.get(int(doc_id))
                output_line = f"{query_number} Q0 {docno} {rank} {score} Exp\n"
                file.write(output_line)
                lines_written += 1
                if lines_written >= 1000:
                    break

    


    def proximity_search(self, query_terms, max_skip_distance=5):
        scores = defaultdict(float)
        for doc_id in self.indexer.doc_ids.values():
            term_positions = {term: self.indexer.postings_map[term]['Postings'][str(doc_id)]['POS'] for term in query_terms if term in self.indexer.postings_map and str(doc_id) in self.indexer.postings_map[term]['Postings']}
            if not term_positions:
                continue
            
            min_distances = []
            for i, term1 in enumerate(query_terms):
                for j, term2 in enumerate(query_terms):
                    if i >= j:
                        continue
                    positions1 = term_positions.get(term1, [])
                    positions2 = term_positions.get(term2, [])
                    for pos1 in positions1:
                        for pos2 in positions2:
                            distance = abs(pos1 - pos2)
                            if distance <= max_skip_distance:
                                min_distances.append(distance)
            
            if min_distances:
                avg_min_distance = sum(min_distances) / len(min_distances)
                score = 1 / (avg_min_distance + 1) 
                scores[doc_id] += score
        
        return scores
    
    def calculate_proximity_score(term_positions_list):
        if not term_positions_list or any(not positions for positions in term_positions_list):
            return 0  
        min_distance = float('inf')
        for i in range(len(term_positions_list)):
            for j in range(i+1, len(term_positions_list)):
                distances = [abs(pos_i - pos_j) for pos_i in term_positions_list[i] for pos_j in term_positions_list[j]]
                if distances:
                    min_distance = min(min_distance, min(distances))
        if min_distance == float('inf'):
            return 0
        return 1 / min_distance

