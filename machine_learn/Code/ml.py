from generate_data import EvaluationData
from sklearn import linear_model
import random
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeClassifier
import numpy as np


class MachineLearn:
    def __init__(self, eval_data, seed=None):
        self.eval = EvaluationData()  # Load data
        self.query_ids = set(self.eval.df["query"])  # Unique query IDs from DataFrame
        self.test_data = []  # Data for testing
        self.train_data = []  # Data for training
        self.eval = eval_data
        self.model = None
        if seed is not None:
            random.seed(seed)  # Set a seed for reproducibility
        self.initialize()

    def initialize(self):
        all_queries = list(self.eval.df["query"].unique())
        random.shuffle(all_queries)  # Shuffle all queries to ensure randomness
        
        self.test_data = all_queries[:5]  # First 5 queries for testing
        self.train_data = all_queries[5:]  # Remaining queries for training
        
        # Check to ensure correct counts
        assert len(self.test_data) == 5, "Test set does not have exactly 5 queries."
        assert len(self.train_data) == 20, "Training set does not have exactly 20 queries."

        print("Test Queries:", self.test_data)
        print("Training Queries:", self.train_data)

    def train(self):
        # Filter DataFrame for training queries only
        train_df = self.eval.df[self.eval.df['query'].isin(self.train_data)]
        X_train = train_df.iloc[:, 2:-1].values  # Assuming features are all columns except 'query', 'doc', and 'label'
        y_train = train_df['label'].values
    
        self.model = LinearRegression() # Train
        self.model.fit(X_train, y_train)

    def predict(self, queries):
        test_df = self.eval.df[self.eval.df['query'].isin(queries)] # Filter DataFrame for specific queries
        X_test = test_df.iloc[:, 2:-1].values # Prepare features
        predictions = self.model.predict(X_test) # Predict using the trained model
        test_df['predicted_score'] = predictions
        test_df.sort_values(by=['query', 'predicted_score'], ascending=[True, False], inplace=True) # Sort results by predicted scores for each query
        return test_df
    
    def test_model(self):
        test_results = self.predict(self.test_data)
        self.format_results(test_results, "./output/test_results.txt")

    def format_results(self, results_df, filename):
        with open(filename, 'w') as f:
            for index, row in results_df.iterrows():
                line = "{} Q0 {} 1 {} Exp\n".format(row['query'], row['doc'], row['predicted_score'])
                # line = "{} Q0 {} 1 {} Exp\n".format(row['query'], row['doc'], row['es'])
                f.write(line)

    def train_performance(self):
        train_results = self.predict(self.train_data)
        self.format_results(train_results, "./output/train_results.txt")

def main():
        # Create an instance of EvaluationData which loads and prepares the data
        eval_data = EvaluationData()
        # Initialize MachineLearn with the evaluation data and an optional seed for reproducibility
        ml = MachineLearn(eval_data, seed=42)
        # Train the model using the training data
        ml.train()
        # Test the model using the testing data
        ml.test_model()
        # Evaluate performance on the training data to check for overfitting
        ml.train_performance()
        # The results are saved to files, which can then be used with treceval or similar tools for evaluation
        print("Testing and training evaluations are complete and results are saved.")

if __name__ == "__main__":
    main()
