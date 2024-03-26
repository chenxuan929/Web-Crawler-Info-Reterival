import os, re
import string
from elasticsearch7 import Elasticsearch, helpers, logger
from nltk.tokenize import word_tokenize
import logging
from concurrent.futures import ProcessPoolExecutor

# For debug use print information
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.WARNING)


def read_stopwords(sw_path):
    '''
    Read the stopwords from file and return as a list
    '''
    stopwords = []
    with open(sw_path, 'r') as file:
        for line in file:
            stopwords.append(line.strip())
    return stopwords


def process_content(text, stopwords):
    '''
    Cleans and processes the input text by removing punctuation,
    converting to lowercase,
    filtering out stopwords,
    mapping words to their stem classes based on a provided mapping
    '''
    text = re.sub(r'[^\w\s.-]|(?<!\S)[.-](?!\S)|(?<=\s)[.-]|(?<=\S)[.-](?=\s)|[.-](?=\Z)', '', text)
    text = re.sub(r'\.(?=\s|$)', ' ', text)
    text = ' '.join([word.lower() for word in text.split() if word.lower() not in stopwords])
    text = map_query_to_stem(text, stem_map)
    return text

def read_stem_classes(file_path):
    '''
    Reads a file containing stem classes
    returns a dictionary mapping each variation to its root form
    '''
    stem_map = {}
    with open(file_path, 'r') as file:
        next(file)  # Skip the header line
        for line in file:
            parts = line.strip().split(' | ')
            if len(parts) == 2:
                root, words = parts
                for word in words.split():
                    stem_map[word] = root
    return stem_map

def map_query_to_stem(query, stem_map):
    '''
    Transforms a query string by replacing each word with its stem form using the provided stem mapping
    '''
    # Split the query into words based on whitespace
    words = query.split()
    
    # Map each word to its stem, if available in the stem_map; otherwise, keep the word as is
    stemmed_words = [stem_map.get(word, word) for word in words]
    
    # Rejoin the stemmed words into a processed query string
    stemmed_query = ' '.join(stemmed_words)
    
    return stemmed_query

def parse_file(file_path, stopwords):
    '''
    Read the file based one its specific format
    Do the pre-processing
    Store its id and content into a dictionary and return
    '''
    text_map = {}
    with open(file_path, 'r', encoding='latin-1') as file:
        doc_id, doc_text = None, ""
        for line in file:
            if line.startswith("<DOCNO>"):
                doc_id = line.strip().split()[1]
            elif line.startswith("<TEXT>"):
                doc_text = ""
            elif line.startswith("</TEXT>"):
                if doc_id:
                    processed_text = process_content(doc_text, stopwords)
                    text_map[doc_id] = processed_text
                    
            elif doc_id:
                doc_text += line
    return text_map

def process_file(file_path, stopwords):
    '''
    Processes a single file, returning a list of actions ready for bulk indexing into Elasticsearch
    '''
    # Process a single file and return a list of actions
    actions = []
    text_map = parse_file(file_path, stopwords)
    for doc_id, doc_text in text_map.items():
        doc_length = len(doc_text.split())
        action = {
            "_index": index_name,
            "_id": doc_id,
            "_source": {
                "content": doc_text,
                "doc_length": doc_length 
            }
        }
        actions.append(action)
    return actions


def bulk_index_documents(es, actions, index_name):
    '''
    Optimize the indexing by using bulk and use log to check is there any error
    '''
    try:
        successes, errors = helpers.bulk(es, actions)
        logger.info(f"Successfully indexed {successes} documents.") 
    except Exception as e:
        logger.error(f"Error during indexing: {str(e)}")


index_name = "ap89_data01"
stem_map = read_stem_classes("./IR_data/AP_DATA/stem-classes.lst")
if __name__ == "__main__":
    # File path needed
    folder = "./IR_data/AP_DATA/ap89_collection/"
    sw_path = './IR_data/AP_DATA/stoplist.txt'
    stopwords = read_stopwords(sw_path)
    #print(stopwords)
    
    es = Elasticsearch()
    if not es.ping():
        raise ValueError("Failed to connect")
    
    configurations = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": {
                "analyzer": {
                    "stopped": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "my_stopwords"]
                    }
                },
                "filter": {
                    "my_stopwords": {
                    "type": "stop",
                    "stopwords": stopwords
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "content": {
                    "type": "text",
                    "fielddata": True,
                    "analyzer": "stopped",
                    "index_options": "positions"
                },
                "doc_length": {
                    "type": "integer"
                }
            }
        }
    }

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name,body=configurations)
    
    total_doc_length = 0
    total_docs = 0
    actions = [] # Store all the indexing process
    
    # Using ProcessPoolExecutor to process files in parallel
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(process_file, os.path.join(folder, filename), stopwords) for filename in os.listdir(folder) if filename.lower() != 'readme']
        
        for future in futures:
            actions.extend(future.result())
    
    bulk_index_documents(es, actions, index_name)