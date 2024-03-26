from elasticsearch7 import Elasticsearch
from document_indexing import process_content, read_stopwords
from nltk.tokenize import word_tokenize
import math
# Average Document Length calc from task2: 224.8161624034578
# total number of documents in the corpus calc from task2: 84678

es = Elasticsearch("http://localhost:9200")
index_name = "ap89_data01"


def search_query(query):
    '''
    Search API
    Match query to find documents containg the search terms
    '''
    result = es.search(index=index_name, query={'match': {'content': query}}, size=1000)
    return result['hits']['hits']



def get_term_vectors(doc_id, term, field):
    '''
    Get vecors and store in term_info: 
    term frequency in the document
    document frequency in the corpus
    total term frequency across all documents.
    '''
    response = es.termvectors(index=index_name, id=doc_id, fields=[field], term_statistics=True)
    term_info = {}
    if field in response['term_vectors']:
        terms = response['term_vectors'][field]['terms']
        if term in terms:
            term_info = {
                'term_freq': terms[term]['term_freq'],
                'doc_freq': terms[term]['doc_freq'],
                'ttf': terms[term].get('ttf', 0)
            }
    return term_info



def get_doc_length(doc_id):
    '''
    Get the doc length previous store in index field
    '''
    try:
        doc = es.get(index=index_name, id=doc_id)
        if 'doc_length' in doc['_source']:
            return doc['_source']['doc_length']
        else:
            print(f"Document {doc_id} does not have 'doc_length' field")
            return 0
    except Exception as e:
        print(f"Error getting document length for {doc_id}: {e}")
        return 0


def get_vocabulary_size():
    '''
    Get the vocabulary size
    '''
    body = {
        "size": 0,
        "aggs": {
            "vocab_size": {
                "cardinality": {
                    "field": "content" ,
                    "precision_threshold": 10000
                }
            }
        }
    }
    result = es.search(index=index_name, body=body)
    vocab_size = result['aggregations']['vocab_size']['value']
    print(f"Vocabulary size: {vocab_size}")
    return vocab_size


def get_total_corpus_length():
    body = {
        "size": 0,
        "aggs": {
            "total_length": {
                "sum": {
                    "field": "doc_length"
                }
            }
        }
    }
    result = es.search(index=index_name, body=body)
    total_length = result['aggregations']['total_length']['value']
    print(f"Total corpus length: {total_length}")
    return total_length



def es_builtin(query, query_number):
    try:
        search_body = {
            "query": {
                "match": {
                    "content": query
                }
            },
            "size": 1000
        }
        result = es.search(index=index_name, body=search_body)
        
        postings = []
        for hit in result['hits']['hits']:
            if hit['_score'] != 0:
                doc_info = {
                    'docno': hit['_id'],
                    'es_builtin_score': hit['_score'],
                    'query_no': query_number
                }
                postings.append(doc_info)

        postings.sort(key=lambda x: x['es_builtin_score'], reverse=True)
        return postings
    
    except Exception as e:
        print(f"An error occurred (will return []): {e}")
        return []


def calc_okapi_tf(term_tf, doc_length, avg_doc_length):
    return term_tf / (term_tf + 0.5 + 1.5 * (doc_length / avg_doc_length)) if avg_doc_length > 0 else 0
    

def okapi_tf(query, query_number, avg_doc_length):
    postings = search_query(query)
    filtered_postings = []
    
    for doc_info in postings:
        doc_id = doc_info['_id']
        tf_score = 0
        doc_length = get_doc_length(doc_id)
        
        for term in query.split():
            term_vectors = get_term_vectors(doc_id, term, 'content')
            term_tf = term_vectors.get('term_freq', 0)
            okapi_tf_score = calc_okapi_tf(term_tf, doc_length, avg_doc_length)
            tf_score += okapi_tf_score
        
        if tf_score != 0:
            doc_info['okapi_tf_score'] = tf_score
            filtered_postings.append(doc_info)
    
    filtered_postings.sort(key=lambda x: x['okapi_tf_score'], reverse=True)
    return filtered_postings[:1000]


def tf_idf(query, query_number, total_docs_in_corpus, avg_doc_length):
    postings = search_query(query)
    filtered_postings = []

    for doc_info in postings:
        doc_id = doc_info['_id']
        doc_length = get_doc_length(doc_id)
        tf_idf_score = 0
        
        for term in query.split():
            term_vectors = get_term_vectors(doc_id, term, 'content')
            term_tf = term_vectors.get('term_freq', 0)
            df_w = term_vectors.get('doc_freq', 0)
            okapi_tf_score = calc_okapi_tf(term_tf, doc_length, avg_doc_length)
            idf = math.log((total_docs_in_corpus / df_w)) if df_w > 0 else 0
            tf_idf_score += okapi_tf_score * idf

        if tf_idf_score != 0:
            doc_info['tf_idf_score'] = tf_idf_score
            filtered_postings.append(doc_info)
    
    filtered_postings.sort(key=lambda x: x['tf_idf_score'], reverse=True)
    return filtered_postings[:1000]


def bm25(query, query_number, total_docs_in_corpus, avg_doc_length, k1=1.2, b=0.75, k2=100):
    postings = search_query(query)
    bm25_postings = []

    for doc_info in postings:
        doc_id = doc_info['_id']
        doc_length = get_doc_length(doc_id)
        okapi_bm25_score = 0

        for term in query.split():
            term_vectors = get_term_vectors(doc_id, term, 'content')
            term_tf = term_vectors.get('term_freq', 0)
            df_w = term_vectors.get('doc_freq', 0)
            
            # Calculate BM25 term score
            idf = math.log(((total_docs_in_corpus + 0.5) / (df_w + 0.5)))
            term_score = idf * ((term_tf * (k1 + 1)) / (term_tf + k1 * (1 - b + b * doc_length / avg_doc_length))) * ((term_tf * (k2 + 1)) / (term_tf + k2))
            if term_score == 0:
                okapi_bm25_score -= 1000
            else:
                okapi_bm25_score += term_score

        if okapi_bm25_score != 0:
            doc_info['okapi_bm25_score'] = okapi_bm25_score
            bm25_postings.append(doc_info)

    bm25_postings.sort(key=lambda x: x['okapi_bm25_score'], reverse=True)
    return bm25_postings[:1000]


def lm_laplace(query, query_number, vocab_size):
    postings = search_query(query)
    laplace_postings = []

    for doc_info in postings:
        doc_id = doc_info['_id']
        doc_length = get_doc_length(doc_id)
        lm_laplace_score = 0

        if doc_length == 0:
            continue
        
        for term in query.split():
            term_tf = get_term_vectors(doc_id, term, 'content').get('term_freq', 0)
            p_laplace = (term_tf + 1) / (doc_length + vocab_size)
            if term_tf == 0:
                lm_laplace_score -= 1000
            else:
                lm_laplace_score += math.log(p_laplace)
            

        if lm_laplace_score != 0:
            doc_info['lm_laplace_score'] = lm_laplace_score
            laplace_postings.append(doc_info)

    laplace_postings.sort(key=lambda x: x['lm_laplace_score'], reverse=True)
    return laplace_postings[:1000]



def lm_jelinek_mercer(query, query_number, total_docs_length, lambda_param=0.5):
    postings = search_query(query)
    jm_postings = []

    for doc_info in postings:
        doc_id = doc_info['_id']
        doc_length = get_doc_length(doc_id)
        lm_jelinek_mercer_score = 0

        if doc_length == 0:
            continue

        for term in query.split():
            term_vectors = get_term_vectors(doc_id, term, 'content')
            term_tf = term_vectors.get('term_freq', 0)
            term_total_tf = term_vectors.get('ttf', 0)

            foreground = term_tf / doc_length
            background = term_total_tf / total_docs_length
            p_jm = lambda_param * foreground + (1 - lambda_param) * background
            if term_tf != 0:
                lm_jelinek_mercer_score += math.log(p_jm)
            else:
               lm_jelinek_mercer_score -= 1000 


        if lm_jelinek_mercer_score != 0:
            doc_info['lm_jelinek_mercer_score'] = lm_jelinek_mercer_score
            jm_postings.append(doc_info)

    jm_postings.sort(key=lambda x: x['lm_jelinek_mercer_score'], reverse=True)
    return jm_postings[:1000]



def run_queries(retrieval_model, queries):
    output_file = f'{retrieval_model}_output.txt'

    avg_doc_length = 224.8161624034578
    total_docs_in_corpus = 84678
    vocab_size = 204506
    total_docs_length = 19037208
    
    with open(output_file, 'w') as output:
        postings = []
        for query_number, query in queries:
            
            print(f"Query Number: {query_number}")
            print(f"Query Text: {query}")
            # if retrieval_model == 'ES_builtin':
                
            #     postings = es_builtin(query, query_number)
            #     # pass
                
            # elif retrieval_model == 'Okapi_TF':
            #     # Okapi TF function here
            #     postings = okapi_tf(query, query_number, avg_doc_length)
            #     # pass
        
            # elif retrieval_model == 'TF_IDF':
            #     # TF-IDF function here
            #     postings = tf_idf(query, query_number, total_docs_in_corpus, avg_doc_length)
            #     # pass
            # elif retrieval_model == 'Okapi_BM25':
            #     # Okapi BM25 function here
            #     postings = bm25(query, query_number, total_docs_in_corpus, avg_doc_length)
            #     # pass
            # elif retrieval_model == 'LM_Laplace':
            #     print("LM_Laplace started")
            #     postings = lm_laplace(query, query_number, vocab_size)
            #     #pass
            # elif retrieval_model == 'LM_Jelinek_Mercer':
            #     print("LM_Jelinek_Mercer started")
            #     postings = lm_jelinek_mercer(query, query_number, total_docs_length)
            #     # pass
            
            # postings = postings[:1000]
            
            # for rank, doc_info in enumerate(postings, start=1):
            #     docno = doc_info.get('docno') or doc_info.get('doc_id') or doc_info.get('_id')
            #     if not docno:
            #         print("Error: docno key not found in doc_info")
            #         continue

            #     retrieval_model_score = f'{retrieval_model.lower()}_score'
            #     output_line = f"{query_number} Q0 {docno} {rank} {doc_info[retrieval_model_score]} Exp\n"
            #     #print(output_line)
            #     output.write(output_line)
                


def parse_queries(file_path, stopwords):
    '''
    Parse queries from a given file, process each query for stopwords and stemming,
    and return a list of processed queries.
    '''
    processed_queries = []
    with open(file_path, 'r') as queries_file:
        for line in queries_file:
            res = line.strip().split('.   ')
            if len(res) == 2:
                query_number, query = res
                # Preprocess the query text
                processed_text = process_content(query, stopwords)
                processed_queries.append((int(query_number), processed_text))
    return processed_queries



if __name__ == "__main__":
    # Getting the total corpus length for future calculate
    #get_total_corpus_length() 
    # output: Total corpus length: 19037208.0

    # Getting the vocabulary size for future calculate
    #get_vocabulary_size()
    # output: Vocabulary size: 204506

    sw_path = './IR_data/AP_DATA/stoplist.txt'
    stopwords = read_stopwords(sw_path)
    queries_file_path = './IR_data/AP_DATA/query_desc.51-100.short.txt'
    processed_queries = parse_queries(queries_file_path, stopwords)


    #print(processed_queries)


# Run queries for each retrieval model
    # retrieval_models = ['ES_builtin', 'Okapi_TF', 'TF_IDF',
    #                 'Okapi_BM25', 'LM_Laplace', 'LM_Jelinek_Mercer']
    retrieval_models = ['ES_builtin']
  
    for model in retrieval_models:
          run_queries(model, processed_queries)


 