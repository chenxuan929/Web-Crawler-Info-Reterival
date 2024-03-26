import os
import re
import pickle
import logging
import nltk
import hashlib
from elasticsearch7 import Elasticsearch, helpers, logger
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from concurrent.futures import ProcessPoolExecutor, as_completed
from elasticsearch7.exceptions import ElasticsearchException

nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
stop_words_set = set(stopwords.words('english'))
ps = PorterStemmer()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_stopwords(sw_path='./stoplist.txt'):
    with open(sw_path, 'r') as file:
        return [line.strip() for line in file]

stop_words = set(read_stopwords())

def get_term_positions(text):
    tokens = tokenize_text(text)
    term_positions = {}
    for position, token in enumerate(tokens):
        term_positions.setdefault(token, []).append(position)
    return term_positions

def tokenize_text(text):
    """Tokenized, convert to lower, remove stopwords, stemming."""
    tokens = word_tokenize(text.lower())
    clean_tokens = [ps.stem(token) for token in tokens if token.isalpha() and token not in stop_words]
    return clean_tokens

def process_file(index_name, file_path, in_links_dict, out_links_dict):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    docno = re.search(r'<DOCNO>(.*?)</DOCNO>', content).group(1).strip()
    title = re.search(r'<HEAD>(.*?)</HEAD>', content).group(1).strip() if re.search(r'<HEAD>(.*?)</HEAD>', content) else ""
    text = re.search(r'<TEXT>(.*?)</TEXT>', content, re.DOTALL).group(1).strip()
    cleaned_text = ' '.join(tokenize_text(text))
    term_positions = get_term_positions(text)
    in_link = in_links_dict.get(docno.strip(), [])
    out_link = out_links_dict.get(docno.strip(), [])
    print(in_link)
    print(out_link)
    documents = [
        {"index": {"_index": index_name}},
        { "id": docno,
             "url": docno,
             "title": title,
             "page_contents_cleaned": cleaned_text,
             "term_positions": term_positions,
             "in_links": in_link,
             "out_links": out_link
     }
    ]
    return documents
    
def create_index(es, index_name):
    if not es.ping():
        raise ValueError("Failed to connect to Elasticsearch.")
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
                        "stopwords": list(stop_words)
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "url": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "stopped"},
                    "page_contents_cleaned": {"type": "text", "analyzer": "stopped"},
                    "term_positions": {"type": "object", "enabled": False},
                    #"raw_html": {"type": "text", "index": False},
                    "in_links": {"type": "keyword"},
                    "out_links": {"type": "keyword"},
                }
            }
        }

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name,body=configurations)

def bulk_index_documents(es, index_name, actions):
    if actions:
        try:
            helpers.bulk(es, index=index_name, actions=actions)
            logger.info(f"Indexed documents successfully.")
        except ElasticsearchException as e:
            logger.error(f"Error indexing documents: {e}")

# def load_links_state(directory_path):
#     with open(os.path.join(directory_path, 'in_links_state.pkl'), 'rb') as f:
#         in_links_dict = pickle.load(f)
#     with open(os.path.join(directory_path, 'out_links_state.pkl'), 'rb') as f:
#         out_links_dict = pickle.load(f)
#     return in_links_dict, out_links_dict

def load_links_state_from_txt(directory_path):
    in_links_dict = {}
    out_links_dict = {}
    with open(os.path.join(directory_path, 'in_links.txt'), 'r', encoding='utf-8') as f:
        for line in f:
            key, val = line.strip().split(': ', 1)
            in_links_dict[key] = val.split(', ') if val else []
    with open(os.path.join(directory_path, 'out_links.txt'), 'r', encoding='utf-8') as f:
        for line in f:
            key, val = line.strip().split(': ', 1)
            out_links_dict[key] = val.split(', ') if val else []
    return in_links_dict, out_links_dict

def main():
    api_key = "QUlYNGJvNEIteS1DYnBqNnZDal86WUdnRVBRckpSV2VzUVc3bjJqdTF2dw=="
    directory_path = './ap89/'
    state_dict = './'
    # in_links_dict, out_links_dict = load_links_state(state_dict)
    in_links_dict, out_links_dict = load_links_state_from_txt(state_dict)
    index_name = "webcrawl_xu2"
    es = Elasticsearch("https://9e930bc5172546d9ab5ee4754db5a0c8.us-central1.gcp.cloud.es.io:443", api_key=api_key)
    create_index(es, index_name)
    for filename in os.listdir(directory_path):
        if filename.endswith('.txt'):
            action = process_file(index_name, os.path.join(directory_path, filename), in_links_dict, out_links_dict)
            bulk_index_documents(es, index_name, action)
if __name__ == "__main__":
    logger.setLevel(logging.WARNING)
    main()
