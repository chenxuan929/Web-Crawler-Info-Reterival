from elasticsearch7 import Elasticsearch
from query_processing import bm25, parse_queries, read_stopwords, get_term_vectors, es_builtin

es = Elasticsearch("http://localhost:9200")
index_name = "ap89_data01"

def get_topK_docs(query, k):
    results = es.search(index=index_name, body={"query": {"match": {"content": query}}, "size": k})
    doc_ids = [hit['_id'] for hit in results['hits']['hits']]
    return doc_ids


def get_term_statistics(doc_ids, field='content'):
    term_stats = {}
    for doc_id in doc_ids:
        result = es.termvectors(index=index_name, id=doc_id, fields=[field], term_statistics=True)
        if field in result['term_vectors']:
            terms = result['term_vectors'][field]['terms']
            for term in terms:
                term_info = get_term_vectors(doc_id, term, field)  
                
                term_stats[term] = {
                    'doc_freq': term_info.get('doc_freq', 0) + term_stats.get(term, {}).get('doc_freq', 0),
                    'ttf': term_info.get('ttf', 0) + term_stats.get(term, {}).get('ttf', 0),
                    
                }
    return term_stats


def select_distinctive_terms(term_stats, top_n=5):
    sorted_terms = sorted(term_stats.items(), key=lambda item: (item[1]['doc_freq'], -item[1]['ttf']), reverse=True)
    return [term for term, stats in sorted_terms[:top_n]]


def expand_query_with_top_docs(original_query, k=5):
    top_docs = get_topK_docs(original_query, k)
    term_stats = get_term_statistics(top_docs)
    distinctive_terms = select_distinctive_terms(term_stats) 
    expanded_query = original_query + " " + " ".join(distinctive_terms)
    return expanded_query




# def run_basicPRF(queries):
#     output_file = 'basicPRF_bm25_output.txt'
#     avg_doc_length = 224.86
#     total_docs_in_corpus = 84678
    
#     with open(output_file, 'w') as output:
#         for query_number, query in queries:
            
#             expanded_query = expand_query_with_top_docs(query)
#             # print("Expanded Query:", expanded_query) # Expanded Query: alleg corrupt public offici offici report new govern say
            
#             final_postings = bm25(expanded_query, query_number, total_docs_in_corpus, avg_doc_length)
#             output_results(final_postings, query_number, output)


def run_basicPRF(queries):
    output_file = 'basicPRF_esbuiltin_output.txt'
    avg_doc_length = 224.86
    total_docs_in_corpus = 84678
    
    with open(output_file, 'w') as output:
        for query_number, query in queries:
            
            expanded_query = expand_query_with_top_docs(query)
            print("Expanded Query:", expanded_query) # Expanded Query: alleg corrupt public offici offici report new govern say
            
            # final_postings = es_builtin(expanded_query, query_number)
            # output_results(final_postings, query_number, output)



def output_results(final_postings, query_number, output_file):
    for rank, doc_info in enumerate(final_postings, start=1):
        docno = doc_info.get('docno') or doc_info.get('doc_id') or doc_info.get('_id')
        if not docno:
            # print("Error: docno key not found in doc_info")
            continue
        score = doc_info.get('es_builtin_score') # change here to run different function
        output_line = f"{query_number} Q0 {docno} {rank} {score} Exp\n"
        # print(output_line)
        output_file.write(output_line)


if __name__ == "__main__":
    sw_path = './IR_data/AP_DATA/stoplist.txt'
    queries_file_path = './IR_data/AP_DATA/query_desc.51-100.short.txt'
    queries = parse_queries(queries_file_path, stopwords = read_stopwords(sw_path))

    run_basicPRF(queries)
