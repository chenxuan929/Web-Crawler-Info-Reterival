from flask import Flask, request, render_template, jsonify
from elasticsearch import Elasticsearch
app = Flask(__name__)

es_client = Elasticsearch(
    "https://14867eb3d54f40d8b122e534783b8ad5.us-central1.gcp.cloud.es.io:443",
    api_key="LVNQanlvNEJUblo0Tm51WVF4ZzQ6ZGZwMW1XeXBRekN1Ty1UYUotNURsdw=="
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['GET'])
def perform_search():
    query = request.args.get('query')
    page = int(request.args.get('page', 0))
    size = 10
    from_ = page * size

    es_query = {
        "from": from_,
        "size": size,
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
    response = es_client.search(index="general_crawler_for_hw4", body=es_query)
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
    response = es_client.search(index="general_crawler_for_hw4", body=es_query)
    
    suggestions = [
        {'url': hit['_source']['url'], 'title': hit['_source']['title']}
        for hit in response['hits']['hits']
    ]
    return jsonify(suggestions)


@app.route('/assess', methods=['POST'])
def assess_documents():
    data = request.get_json()
    assessments = data.get('assessments', [])
    assessor_id = data.get('assessor_id', 'unknown')

    with open('assessments.qrel', 'a') as file:
        for assessment in assessments:
            # line format: QueryID AssessorID DocID Grade
            line = f"{assessment.get('query_id', 'unknown')} {assessor_id} {assessment['url']} {assessment['grade']}\n"
            file.write(line)

    return jsonify({"message": "Assessments submitted successfully"})

if __name__ == '__main__':
    app.run(debug=True)
