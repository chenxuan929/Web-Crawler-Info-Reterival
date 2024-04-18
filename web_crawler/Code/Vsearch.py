from flask import Flask, request, render_template, jsonify
from elasticsearch import Elasticsearch
app = Flask(__name__)

es_client = Elasticsearch(
    "https://9e930bc5172546d9ab5ee4754db5a0c8.us-central1.gcp.cloud.es.io:443",
    api_key="QUlYNGJvNEIteS1DYnBqNnZDal86WUdnRVBRckpSV2VzUVc3bjJqdTF2dw=="
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['GET'])
def perform_search():
    query = request.args.get('query')
    es_query = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["title", "text"]
            }
        },
        "highlight": {
            "fields": {
                "text": {}
            }
        }
    }
    response = es_client.search(index="general_crawler", body=es_query)
    results = [
        {
            'url': hit['_source']['url'],
            'title': hit['_source']['title'],
            'snippet': hit['highlight']['text'][0] if 'highlight' in hit else ''
        } for hit in response['hits']['hits']
    ]
    return jsonify(results)

@app.route('/autocomplete', methods=['GET'])
def autocomplete():
    query = request.args.get('query', '').lower()
    es_query = {
        "query": {
            "bool": {
                "should": [
                    {"prefix": {"title": {"value": query}}},
                    {"prefix": {"text": {"value": query}}}
                ]
            }
        },
        "_source": ["title", "url"],
        "size": 5
    }
    response = es_client.search(index="general_crawler", body=es_query)
    
    suggestions = [
        {'url': hit['_source']['url'], 'title': hit['_source']['title']}
        for hit in response['hits']['hits']
    ]
    return jsonify(suggestions)

if __name__ == '__main__':
    app.run(debug=True)
