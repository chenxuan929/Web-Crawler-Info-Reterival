from elasticsearch import Elasticsearch

class ES:
    def __init__(self):
        self.index = "general_crawler_for_hw4"
        self.es = Elasticsearch(
            "https://14867eb3d54f40d8b122e534783b8ad5.us-central1.gcp.cloud.es.io:443",
            api_key="LVNQanlvNEJUblo0Tm51WVF4ZzQ6ZGZwMW1XeXBRekN1Ty1UYUotNURsdw=="
            )
        self.qrel = {"150901": {}, "150902": {}, "150903": {}, "150904": {}}
        self.rank_list = {"150901": [], "150902": [], "150903": [], "150904": []}
        self.query = ["Brezhnev Doctrine", "Sino-Soviet split", "cuban missile crisis", "poland solidarity"]
        self.query_id = ["150901", "150902", "150903", "150904"]

    def get_rank_list(self):
        for idx, q in enumerate(self.query):
            print(f"Reading ranked list for: {q}")
            response = self.es.search(index=self.index, body={
                "size": 1000,
                "query": {
                    "multi_match": {
                        "query": q,
                        "fields": ["title", "text"],
                        "type": "best_fields"
                    }
                },
                "_source": ["url", "_score"]
            })
            if 'hits' in response:
                for hit in response['hits']['hits']:
                    url = hit['_source']['url']
                    score = hit['_score']
                    if self.query_id[idx] not in self.rank_list:
                        self.rank_list[self.query_id[idx]] = []
                    self.rank_list[self.query_id[idx]].append({url: score})

    def output_rank_list(self):
        with open("./Results/ranked_list.txt", "a", encoding="utf-8") as f:
            for q_id, docs in self.rank_list.items():
                for idx, doc in enumerate(docs):
                    for url, score in doc.items():
                        line = f"{q_id} Q0 {url} {idx+1} {score} Exp\n"
                        f.write(line)

if __name__ == "__main__":
    my_es = ES()
    my_es.get_rank_list()
    my_es.output_rank_list()
