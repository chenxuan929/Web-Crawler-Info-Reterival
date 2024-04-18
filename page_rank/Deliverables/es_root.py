from elasticsearch7 import Elasticsearch
from hit import Hits

class RootSetFetcher:
    def __init__(self, query="Sino-Soviet split", expansion_limit=200): 
        #uses Elasticsearch's search API to query the index with my specified topic
        self.es_host = "https://9e930bc5172546d9ab5ee4754db5a0c8.us-central1.gcp.cloud.es.io:443"
        self.api_key = "QUlYNGJvNEIteS1DYnBqNnZDal86WUdnRVBRckpSV2VzUVc3bjJqdTF2dw=="
        self.index_name = "general_crawler_for_hw4"
        self.es = Elasticsearch(self.es_host, api_key=self.api_key)
        self.query = query
        self.root_set_ids = []
        self.expansion_limit = expansion_limit
        self.base_set_ids = set()

    def fetch_root_set(self, result_size=1000):
        search_result = self.es.search(
            index=self.index_name,
            body={
                "from": 0,
                "size": result_size,
                "query": {
                    "match": {
                        "text": self.query
                    }
                },
                "_source": False
            }
        )['hits']['hits']
        self.root_set_ids = [item['_id'] for item in search_result]
        print(f"Fetched {len(self.root_set_ids)} root set IDs based on my project topic: '{self.query}'")

        
def main():
    fetcher = RootSetFetcher(query="Sino-Soviet split")
    fetcher.fetch_root_set(result_size=1000)
    root_set_ids = fetcher.root_set_ids
    hits_processor = Hits(root_set_ids)
    hits_processor.expand_base_set(target_size=10000, max_iterations=3) 
    hits_processor.compute_hits_scores()
    hits_processor.save_top_500(hits_processor.authority_scores, "top_500_authorities.txt")
    hits_processor.save_top_500(hits_processor.hub_scores, "top_500_hubs.txt")

if __name__ == "__main__":
    main()