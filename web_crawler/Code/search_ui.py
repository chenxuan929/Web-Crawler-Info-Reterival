from flask import Flask, request, render_template, jsonify
from elasticsearch7 import Elasticsearch
import os

app = Flask(__name__)
es = Elasticsearch(os.environ.get('ES_CLOUD_HOST'), api_key = os.environ.get('ES_CLOUD_API_KEY'))
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('query')
    response = es.search(index="search-hw3-wy", query = {"multi_match": {"query": query, "fields": ["title", "content"]}})
    results = [{'url': hit['_source']['url'], 'title': hit['_source']['title']} for hit in response['hits']['hits']]
    return jsonify(results)

@app.route('/autocomplete', methods=['GET'])
def autocomplete():
    query = request.args.get('query', '')
    # Assuming you have a "title" field in your documents
    response = es.search(index="search-hw3-wy", 
        query={
            "prefix": {
                "title": {
                    "value": query.lower()
                }
            }
        },
        _source = ["title", 'url'],  # Only return the "title" field
        size = 5  # Limit the number of suggestions
    )
    
    # Extract titles for suggestions
    suggestions = [{'url': hit['_source']['url'], 'title': hit['_source']['title']} for hit in response['hits']['hits']]
    return jsonify(suggestions)

if __name__ == '__main__':
    app.run(debug=True)