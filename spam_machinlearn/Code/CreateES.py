from elasticsearch7 import Elasticsearch, helpers
import json

class EmailES:
    def __init__(self, texts=None, labels=None, splits=None):
        self.es = Elasticsearch(timeout=60) #  Elasticsearch runs at http://localhost:9200
        if not self.es.ping():
            raise ValueError("Failed to connect")
    
        self.term_vectors = {}
        self.spam_related_words = []
        self.features = {}

        if texts and labels and splits:
            self.texts = texts
            self.labels = labels
            self.splits = splits
        else:
            self.load_data_from_json()

    def load_data_from_json(self):
        try:
            with open("./processed_emails.json", "r") as file:
                self.texts = json.load(file)
            with open("./email_labels.json", "r") as file:
                self.labels = json.load(file)
            with open("./data_splits.json", "r") as file:
                self.splits = json.load(file)
            print("Data loaded from JSON.")
        except IOError as e:
            print(f"File error: {e}")
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")

    def create_index(self):
        self.es.indices.delete(index="emails", ignore=[400, 404])  # Ignore error if the index does not exist
        self.es.indices.create(index="emails", body={
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "text": {"type": "text"},
                    "spam": {"type": "keyword"},
                    "split": {"type": "keyword"}
                }
            }
        })
        actions = [
            {
                "_index": "emails",
                "_id": id[7:],
                "_source": {
                    "id": id[7:],
                    "text": self.texts[id],
                    "spam": self.labels[id],
                    "split": self.splits[id]
                }
            } for id in self.labels
        ]
        helpers.bulk(self.es, actions)

    def fetch_term_vectors(self, ids):
        try:
            res = self.es.mtermvectors(index="emails", body={"ids": ids}, fields=["text"],
                                    field_statistics=False, payloads=False, offsets=False,
                                    positions=False)
            term_vectors = {}
            for item in res["docs"]:
                doc_id = item['_id']
                if "text" in item["term_vectors"]:
                    term_vector = item["term_vectors"]["text"]["terms"]
                    term_vectors[doc_id] = term_vector
                else:
                    print(f"No term vectors for ID: {doc_id}")
                    continue
            with open("./term_vectors.json", "w") as f:
                json.dump(term_vectors, f)
        except Exception as e:
            print(f"Error fetching term vectors: {e}")

    def load_term_vectors(self):
        with open("./term_vectors.json", "r") as file:
            self.term_vectors = json.load(file)
        print("Term vectors loaded.")

    def load_spam_related_words(self):
        with open("./spam_words.txt", "r") as file:
            words = file.read().split()
            self.spam_related_words = [word.lower() for word in words]
        print("Spam related words loaded.")

    def extract_features(self):
        for word in self.spam_related_words:
            results = helpers.scan(self.es, query={"query": {"match": {"text": word}}}, index="emails", _source=["id"])
            target_ids = [result["_source"]["id"] for result in results]
            for id in target_ids:
                if id in self.term_vectors and word in self.term_vectors[id]:
                    term_freq = self.term_vectors[id][word]["term_freq"]
                    if word not in self.features:
                        self.features[word] = {}
                    self.features[word][id] = term_freq
        print("Features extracted for spam related words.")


if __name__ == "__main__":
    email_search = EmailES()
    #email_search.create_index()
