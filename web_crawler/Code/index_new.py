from elasticsearch7 import Elasticsearch, helpers
import logging
import os
import re
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ps = PorterStemmer()

def read_stopwords(sw_path='./stoplist.txt'):
    with open(sw_path, 'r') as file:
        return [line.strip() for line in file]
stop_words = set(read_stopwords())


def tokenize_text(text):
    """Tokenized, convert to lower, remove stopwords, stemming."""
    tokens = word_tokenize(text.lower())
    clean_tokens = [ps.stem(token) for token in tokens if token.isalpha() and token not in stop_words]
    return clean_tokens


class Indexer:
    def __init__(self, output_dir, es_host, index_name, api_key):
        self.output_dir = output_dir
        self.es = Elasticsearch(es_host, api_key=api_key)
        self.index_name = index_name
        self.create_index()

    def create_index(self):
        if not self.es.ping():
            raise ValueError("Failed to connect to Elasticsearch.")
        settings = {
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
                    "url": {"type": "keyword"},
                    "title": {"type": "text"},
                    "text": {"type": "text"},
                    "in_links": {"type": "keyword"},
                    "out_links": {"type": "keyword"},
                    "header": {"type": "text"}
                }
            }
        }

        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
        self.es.indices.create(index=self.index_name, body=settings)

    def index_documents(self):
        actions = []
        documents_path = os.path.join(self.output_dir, "documents")
        links_path = os.path.join(self.output_dir, "links")

        for filename in os.listdir(documents_path):
            if filename.endswith('.txt'):
                parts = filename.split('_')
                doc_id = parts[-1].split('.')[0]
                with open(os.path.join(documents_path, filename), 'r', encoding='utf-8') as f:
                    content = f.read()
                    url_match = re.search(r'<DOCNO>(.*?)</DOCNO>', content)
                    title_match = re.search(r'<HEAD>(.*?)</HEAD>', content)
                    text_match = re.search(r'<TEXT>(.*?)</TEXT>', content, re.DOTALL)

                url = url_match.group(1).strip() if url_match else ""
                title = title_match.group(1).strip() if title_match else ""
                text = text_match.group(1).strip() if text_match else ""
                cleaned_text = ' '.join(tokenize_text(text))
                url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
                links_file = os.path.join(links_path, f"links_graph_{doc_id.split('_')[-1]}.txt")
                headers_path = os.path.join(self.output_dir, "headers")
                header_file_path = os.path.join(headers_path, f"headers_{doc_id}.txt")
                try:
                    with open(header_file_path, 'r', encoding='utf-8') as header_file:
                        header_content = header_file.read()
                except FileNotFoundError:
                    logger.error(f"Header file not found for document {doc_id}: {header_file_path}")
                    header_content = ""

                try:
                    in_links, out_links = self.read_links(links_file)
                    logging.info("Links found")
                except FileNotFoundError:
                    #logger.error(f"Links file not found for document {doc_id}: {links_file}. Skipping document.")
                    in_links, out_links = [], []
                    continue

                action = {
                    "_index": self.index_name,
                    "_id": url_hash,
                    "_source": {
                        "url": url,
                        "title": title,
                        "text": cleaned_text,
                        "in_links": list(in_links),
                        "out_links": list(out_links),
                        "header": header_content,
                    }
                }
                actions.append(action)

                if len(actions) == 500:
                    helpers.bulk(self.es, actions)
                    actions = []

        if actions:
            helpers.bulk(self.es, actions)

    @staticmethod
    def read_links(links_file):
        in_links = set()
        out_links = set()
        try:
            with open(links_file, 'r', encoding='utf-8') as f:
                content = f.read()
                if "Inlinks:" in content:
                    in_links = set(content.split("Inlinks:")[1].split(", ")[0].split())
                if "Outlinks:" in content:
                    out_links = set(content.split("Outlinks:")[1].split(", ")[0].split())
        except FileNotFoundError:
            logger.error(f"Links file not found: {links_file}")
        return in_links, out_links

if __name__ == "__main__":
    output_directory = './Results/'
    es_host = "https://9e930bc5172546d9ab5ee4754db5a0c8.us-central1.gcp.cloud.es.io:443"
    index_name = "craw_chenxuan_xu_2"
    api_key = "QUlYNGJvNEIteS1DYnBqNnZDal86WUdnRVBRckpSV2VzUVc3bjJqdTF2dw=="

    indexer = Indexer(output_directory, es_host, index_name, api_key)
    indexer.index_documents()
