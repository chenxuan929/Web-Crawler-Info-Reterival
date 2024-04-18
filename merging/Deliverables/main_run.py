from retrieval_models import RetrievalModels
from tokenizing import Tokenizing
from indexing import Indexer
from parser_doc import TextProcessingAndParsing
import logging, os
from nltk.stem.porter import PorterStemmer
import re

logging.basicConfig(level=logging.INFO)




def check_index_size(output_dir):
    num_files = len([name for name in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, name))])
    print(f"Number of files in the output directory: {num_files}")
    total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, f)))
    total_size = float(total_size / (1024 * 1024))
    print(f"Total size of the files in the output directory: {total_size} mb")
    #Total size of the files in the output directory: 167.90258407592773 mb
    return num_files, total_size


def process_queries(queries_path, stopwords_path, tokenizer):
        query_pattern = re.compile(r'^(\d+)\.\s+(.+)$') 
    
        queries_map = {}
        with open(stopwords_path, 'r') as f:
            stopwords = set(f.read().splitlines())
    
        with open(queries_path, 'r') as file:
            for line in file:
                match = query_pattern.match(line.strip())
                if match:
                    query_id, text = match.groups()
                    # Tokenize the query text using the Tokenizing instance
                    tokens_with_ids = tokenizer.tokenize(query_id, text)
                    # Filter out stopwords and single-character words after tokenizing
                    filtered_tokens = [term_id for term_id, _, _ in tokens_with_ids if tokenizer.idTerm_map[term_id] not in stopwords and len(tokenizer.idTerm_map[term_id]) > 1]
                    queries_map[query_id] = filtered_tokens
        #print(queries_map)  
        return queries_map


def main_runner(if_stemm, if_compress):
    logging.info(f'Prepare Processing a {if_stemm} Stemming and {if_compress} Compressed Indexing')

    stopwords_path = "./Resources/stoplist.txt"
    queries_path = "./IR_data/AP_DATA/query_desc.51-100.short.txt"
    path = "./IR_data/AP_DATA/ap89_collection/"
    parser = TextProcessingAndParsing(path, stopwords_path, if_stemm)
    parsed_files = parser.process_documents(os.cpu_count())
    indexer = Indexer(if_stemm = if_stemm, maxDocs=len(parsed_files))
    

    logging.info("Indexing Started.")
    for doc_id, text in parsed_files.items():
        indexer.index_doc(doc_id, text)

    if if_compress:
        logging.info("Start Mergging partial index and Compress them.")
        
    else:
        logging.info("Start Mergging partial index with no Compress after.")
    postings_map = indexer.merge(if_compress)
    #print(postings_map)
    
 
    logging.info("Start Saving and writing info to disk.")
    indexer.writeDisk_meta()

    merged_idx_dir = "idx_output/doStem"
    check_index_size(merged_idx_dir)


    logging.info("Start querying.")
    tokenizer_instance = Tokenizing()
    queries_map = process_queries(queries_path, stopwords_path, tokenizer_instance)


    logging.info("Start Calculating score for modles")
    retrievalModels = RetrievalModels(indexer)
    # #print(indexer.doc_len_map)
    # doc_len_map = indexer.doc_len_map

    # for query_id, terms in queries_map.items():
    #     #print(query_id, terms)
    #     scores_bm25 = {}
    #     scores_tfidf = {}
    #     scores_lg = {}

    #     for term_id in terms:
    #         if str(term_id) in postings_map:
    #             doc_ids = postings_map[str(term_id)]
    #             #print(f"doc ids: {doc_ids}\n\n\n")
    #             for doc_id, _ in doc_ids.items():
    #                 doc_length = doc_len_map.get(int(doc_id))
    #                 #print(f"doc length: {doc_length}\n")
    #                 # BM25
    #                 score_bm25 = retrievalModels.bm25(term_id, doc_id, doc_length)
    #                 #print(f"score bm25: {score_bm25}\n")
    #                 scores_bm25[doc_id] = scores_bm25.get(doc_id, 0) + score_bm25
                    
            
    #                 # TF-IDF
    #                 score_tfidf = retrievalModels.tf_idf(term_id, doc_id, doc_length)
    #                 scores_tfidf[doc_id] = scores_tfidf.get(doc_id, 0) + score_tfidf
                    
            
    #                 # Language Model with Laplace Smoothing
    #                 score_lg = retrievalModels.lm_laplace(term_id, doc_id, doc_length)
    #                 scores_lg[doc_id] = scores_lg.get(doc_id, 0) + score_lg
                    

                    
    #         else:
    #             print(f"Term {term_id} not found in index.")

    #     retrievalModels.write_results(query_id, scores_bm25, "BM25")
    #     retrievalModels.write_results(query_id, scores_tfidf, "TFIDF")
    #     retrievalModels.write_results(query_id, scores_lg, "LG")

    # for query_id, terms in queries_map.items():
    #     score_proximity = {}
    #     proximity_scores = retrievalModels.proximity_search(terms, max_skip_distance=5)
    #     retrievalModels.write_results(query_id, score_proximity, "Prox")


if __name__ == "__main__":
    main_runner(True, False)
    #main_runner(True, False)
    #main_runner(False, False)


# Total size of the files in the output directory: 167.90667152404785 mb
# 