# import pandas as pd
# from elasticsearch7 import Elasticsearch, ElasticsearchException
# from sklearn.feature_extraction.text import CountVectorizer
# from sklearn.linear_model import LogisticRegression
# from sklearn.metrics import classification_report, roc_auc_score
# import scipy.sparse
# import joblib  # Correctly imported joblib

# class TextClassifier:
#     def __init__(self):
#         self.es = Elasticsearch("http://localhost:9200")
#         if not self.es.ping():
#             raise ValueError("Elasticsearch is not connected. Please check your server.")
#         self.vectorizer = CountVectorizer(analyzer='word', token_pattern=r'\b\w+\b')

#     def fetch_data(self, index_name="emails"):
#         try:
#             response = self.es.search(index=index_name, query={"match_all": {}}, size=10000)
#             texts = [hit['_source']['text'] for hit in response['hits']['hits']]
#             labels = [hit['_source']['spam'] for hit in response['hits']['hits']]
#             return texts, labels
#         except ElasticsearchException as e:
#             raise Exception(f"Failed to fetch data: {e}")

#     def prepare_data(self, texts, labels):
#         data_split_boundary = int(len(texts) * 0.8)
#         train_texts = texts[:data_split_boundary]
#         test_texts = texts[data_split_boundary:]
#         train_labels = labels[:data_split_boundary]
#         test_labels = labels[data_split_boundary:]

#         self.train_data = self.vectorizer.fit_transform(train_texts)
#         self.test_data = self.vectorizer.transform(test_texts)

#         joblib.dump(self.vectorizer, 'vectorizer.pkl')
#         scipy.sparse.save_npz('train_data.npz', self.train_data)
#         scipy.sparse.save_npz('test_data.npz', self.test_data)

#         return train_labels, test_labels

#     def train_and_evaluate(self, train_labels, test_labels):
#         model = LogisticRegression(max_iter=1000, solver='liblinear')
#         model.fit(self.train_data, train_labels)
#         predictions = model.predict(self.test_data)

#         feature_importances = sorted(zip(self.vectorizer.get_feature_names_out(), model.coef_[0]), key=lambda x: -abs(x[1]))[:10]
#         print("Top 10 important features:")
#         for feature, importance in feature_importances:
#             print(f"{feature}: {importance}")

#         print("Classification Report:")
#         print(classification_report(test_labels, predictions))
#         print("ROC AUC Score:", roc_auc_score(test_labels, model.decision_function(self.test_data)))


#     def run(self):
#         texts, labels = self.fetch_data()
#         train_labels, test_labels = self.prepare_data(texts, labels)
#         self.train_and_evaluate(train_labels, test_labels)

# if __name__ == "__main__":
#     classifier = TextClassifier()
#     classifier.run()


import pandas as pd # for data manipulation
from elasticsearch7 import Elasticsearch, ElasticsearchException # querying data from an Elasticsearch index
from sklearn.feature_extraction.text import CountVectorizer # text vectorization and machine learning
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import MultinomialNB
from sklearn.tree import DecisionTreeClassifier
import scipy.sparse # handling sparse matrices
import joblib # saving and loading Python objects efficiently

class TextClassifier:
    def __init__(self):
        self.es = Elasticsearch("http://localhost:9200")
        if not self.es.ping():
            raise ValueError("Elasticsearch is not connected. Please check your server.")
        self.vectorizer = CountVectorizer(analyzer='word', token_pattern=r'\b\w+\b') # converting text data into a format that machine learning models can process

    def fetch_data(self, index_name="emails"): 
        # get data from es to build a dara frame
        try:
            response = self.es.search(index=index_name, query={"match_all": {}}, size=10000)
            self.df = pd.DataFrame([{
                'id': hit['_id'],
                'text': hit['_source']['text'],
                'spam': hit['_source']['spam']
            } for hit in response['hits']['hits']])
            self.df['split'] = ['train' if i < len(self.df) * 0.8 else 'test' for i in range(len(self.df))]
            return self.df
        except ElasticsearchException as e:
            raise Exception(f"Failed to fetch data: {e}")

    def prepare_data(self):
        # separates the DataFrame into training and testing sets
        train_df = self.df[self.df['split'] == 'train']
        test_df = self.df[self.df['split'] == 'test']
        # turn data to an objct -> vectorizes the text data
        self.train_data = self.vectorizer.fit_transform(train_df['text'])
        self.test_data = self.vectorizer.transform(test_df['text'])
        joblib.dump(self.vectorizer, 'vectorizer.pkl')
        scipy.sparse.save_npz('train_data.npz', self.train_data)
        scipy.sparse.save_npz('test_data.npz', self.test_data)
        return train_df['spam'], test_df['spam']

    def train_and_evaluate(self, train_labels, test_labels):
        # Define a dictionary of models with their corresponding initialization settings
        models = {
            'Linear Regression': LogisticRegression(max_iter=1000, solver='liblinear'),
            'Naive Bayes': MultinomialNB(),
            'Decision Tree': DecisionTreeClassifier()
        }
        # Iterate through each model, train it, make predictions, and evaluate
        for name, model in models.items():
            model.fit(self.train_data, train_labels)
            predictions = model.predict(self.test_data)
            probabilities = model.predict_proba(self.test_data)[:, 1] if hasattr(model, "predict_proba") else model.decision_function(self.test_data)
            self.output_result(test_labels, predictions, probabilities, name)

    def output_result(self, actual_labels, predictions, probabilities, model_name):
        test_ids = self.df[self.df['split'] == 'test']['id']
        results_df = pd.DataFrame({
            'id': test_ids,
            'actual': actual_labels,
            'predict': predictions,
            'probability': probabilities
        })
        results_df.sort_values(by='probability', ascending=False, inplace=True)
        results_df['probability'] = results_df['probability'].apply(lambda x: format(x, '.5f'))
        results_df.to_csv(f'{model_name}_predictions.txt', index=False, header=True)
        print(f"Results for {model_name} saved to '{model_name}_predictions.txt'.")

    def run(self):
        self.fetch_data()
        train_labels, test_labels = self.prepare_data()
        self.train_and_evaluate(train_labels, test_labels)

if __name__ == "__main__":
    classifier = TextClassifier()
    classifier.run()

