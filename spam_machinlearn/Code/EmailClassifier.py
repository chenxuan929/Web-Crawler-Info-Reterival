import pandas as pd
from elasticsearch7 import Elasticsearch, helpers, ElasticsearchException
from sklearn.linear_model import LinearRegression
from sklearn.naive_bayes import MultinomialNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import classification_report

class SpamClassifier:
    def __init__(self):
        self.es = Elasticsearch("http://localhost:9200")
        if not self.es.ping():
            raise ValueError("Elasticsearch is not connected. Please check your server.")
        self.spam_related_words = self.load_spam_words()
        self.features = {} # store features extracted from Elasticsearch later

    def load_spam_words(self):
        with open("my_spam.txt", "r") as file:
            words = file.read().split()
        return [word.lower() for word in words]

    def fetch_features_from_es(self, index_name="emails"):
        # checking the words matches the spam words and 
        for word in self.spam_related_words:
            try:
                response = self.es.search(
                    index=index_name,
                    size=0, # not return actual documents, only aggregation data
                    query={
                        "match_phrase": {"text": word}
                    },
                    aggs={
                        "docs": {
                            "terms": {"field": "_id", "size": 9999}
                        }
                    }
                )
                # if the filter do get results and current doc within the filter result
                if 'aggregations' in response and 'docs' in response['aggregations']:
                    self.features[word] = {doc['key']: doc['doc_count'] for doc in response['aggregations']['docs']['buckets']}
            except ElasticsearchException as e:
                print(f"Failed to fetch features for {word}: {e}")

    def prepare_data_frame(self, index_name="emails"):
        ids = [hit['_id'] for hit in self.es.search(index=index_name, query={"match_all": {}}, size=10000)['hits']['hits']]
        labels = {hit['_id']: hit['_source']['spam'] for hit in self.es.search(index=index_name, query={"match_all": {}}, _source=["spam"], size=10000)['hits']['hits']}
        # convert the spam lable to 0 and 1 in df
        data = {"id": ids, "label": [1 if labels[id] == "spam" else 0 for id in ids]}
        for word in self.spam_related_words:
            data[word] = [self.features.get(word, {}).get(id, 0) for id in ids]
        
        self.data_frame = pd.DataFrame(data)
        self.train_set = self.data_frame.sample(frac=0.8, random_state=200)  # 80% training data
        self.test_set = self.data_frame.drop(self.train_set.index)  # 20% testing data

    def train_models(self):
        self.models = { # three machine learning models
            "Linear Regression": LinearRegression(),
            "Naive Bayes": MultinomialNB(),
            "Decision Tree": DecisionTreeClassifier()
        }
        for name, model in self.models.items():
            model.fit(self.train_set.drop(columns=['id', 'label']), self.train_set['label'])
            if name == "Linear Regression":
                predictions = model.predict(self.test_set.drop(columns=['id', 'label']))
                predictions = [1 if x > 0.5 else 0 for x in predictions]  # Threshold predictions
            else:
                predictions = model.predict(self.test_set.drop(columns=['id', 'label']))
            print(f"Results for {name}:")
            print(classification_report(self.test_set['label'], predictions))

    def run(self):
        self.fetch_features_from_es()
        self.prepare_data_frame()
        self.train_models()

if __name__ == "__main__":
    classifier = SpamClassifier()
    classifier.run()

