from elasticsearch7 import Elasticsearch
from elasticsearch7 import helpers


class Get_From_ES():
    def __init__(self):
        self.es_host = "https://9e930bc5172546d9ab5ee4754db5a0c8.us-central1.gcp.cloud.es.io:443"
        self.api_key = "QUlYNGJvNEIteS1DYnBqNnZDal86WUdnRVBRckpSV2VzUVc3bjJqdTF2dw=="
        self.index_name = "general_crawler_for_hw4"
        self.es = Elasticsearch(self.es_host, api_key=self.api_key)
        self.in_links = {}
        self.out_links = {}

    def search_in_link(self):
        docs = helpers.scan(self.es, index=self.index_name, query={
                            "query": {
                                "match_all": {}
                            },
                            "_source": ["in_links"]
                            },
                            size=2000,
                            request_timeout=30)
        count = 0
        for each in docs:
            count += 1
            url = each["_id"]
            in_links_get = each["_source"]["in_links"]
            self.in_links[url] = in_links_get
            print(f"Found {count} item in in link docs")

    def search_out_link(self):
        docs = helpers.scan(self.es, index=self.index_name, query={
                            "query": {
                                "match_all": {}
                            },
                            "_source": ["out_links"]
                            },
                            size=2000,
                            request_timeout=30)
        count = 0
        for each in docs:
            count += 1
            url = each["_id"]
            out_links_get = each["_source"]["out_links"]
            self.out_links[url] = out_links_get
            print(f"Found {count} item in out link docs")

    def write_in_link(self):
        with open("./links/in_link.txt", "a") as file:
            for url in self.in_links:
                line = "{} ".format(url)
                for il in self.in_links[url]:
                    line += "{} ".format(il)
                file.write(line)
                file.write("\n")

    def write_out_link(self):
        with open("./links/out_link.txt", "a") as file:
            for url in self.out_links:
                line = "{} ".format(url)
                for il in self.out_links[url]:
                    line += "{} ".format(il)
                file.write(line)
                file.write("\n")


def main():
    es = Get_From_ES()
    print("scucess connect to es, start search in link.")
    #es.search_in_link()
    #es.write_in_link()
    print("Start search out link.")
    es.search_out_link()
    es.write_out_link()


if __name__ == "__main__":
    main()
