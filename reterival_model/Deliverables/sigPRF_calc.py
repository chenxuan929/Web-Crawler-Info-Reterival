from elasticsearch7 import Elasticsearch
from query_processing import bm25, parse_queries, read_stopwords, es_builtin
from document_indexing import read_stopwords


es = Elasticsearch("http://localhost:9200")
index_name = "ap89_data01"


def get_significant_terms_for_query_terms(query_terms, k):
    significant_terms = {}
    # For each term in the analyzed query, find significant terms related to it
    for term in query_terms:
        response = es.search(
            index=index_name,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"content": term}}
                        ]
                    }
                },
                "aggs": {
                    "significantTerms": {
                        "significant_terms": {"field": "content", "size": k}
                    }
                },
                "size": 0
            }
        )
        # Accumulate scores or counts for each significant term
        for bucket in response['aggregations']['significantTerms']['buckets']:
            key = bucket['key']
            if key not in significant_terms:
                significant_terms[key] = 1
            else:
                significant_terms[key] += 1

    # Optionally filter terms to ensure they are related to more than one query term
    # and meet other specified criteria like not being stopwords, having high IDF, etc.
    filtered_terms = [term for term,
                      count in significant_terms.items() if count > 1]

    return filtered_terms[:k]


def analyze_query(query):
    '''
    Analyze API
    Get the tokens that use for searching
    '''
    body = {"text": query}
    result = es.indices.analyze(index=index_name, body=body)
    tokens = [token_info['token'] for token_info in result['tokens']]
    return tokens

def run_sig_prf(queries):
    output_file = 'sigPRF_esbuiltin_output.txt' # change here to run different function
    avg_doc_length = 224.86
    total_docs_in_corpus = 84678

    with open(output_file, 'w') as output:
        for query_number, query in queries:
            query_terms = analyze_query(query) 
            k = 5
            significant_terms = get_significant_terms_for_query_terms(
                query_terms, k)
            expanded_query = query + " " + " ".join(significant_terms)
            #print("Expanded Query:", expanded_query)

            final_postings = es_builtin(expanded_query, query_number) # change here to run different function
            for rank, doc_info in enumerate(final_postings, start=1):
                docno = doc_info.get('_id')
                score = doc_info.get('es_builtin_score') # change here to run different function
                output_line = f"{query_number} Q0 {docno} {rank} {score} Exp\n"
                output.write(output_line)


if __name__ == "__main__":
    sw_path = './IR_data/AP_DATA/stoplist.txt'
    queries_file_path = './IR_data/AP_DATA/query_desc.51-100.short.txt'
    queries = parse_queries(queries_file_path, stopwords = read_stopwords(sw_path))
    sw_path = './IR_data/AP_DATA/stoplist.txt'

    
    run_sig_prf(queries)