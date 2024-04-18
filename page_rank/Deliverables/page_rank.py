import math
import os
import numpy as np
import hashlib


class PageRank():

    def __init__(self):
        self.in_links = {}  # Page -> In-links
        self.out_links = {}  # Page -> Out-links
        self.all_pages = []  # List of all pages
        self.sink = []  # Sink pages, i.e., pages with no out-links
        self.num_ol = {}  # Page -> Number of out-links
        self.damp = 0.85  # Damping factor for PageRank
        self.pr = {}  # Page -> PageRank score
        self.initialize()

    def initialize(self):
        self.read_in_links()
        self.read_out_links()
        self.all_pages = list(self.in_links.keys())
        self.num_ol = len(self.all_pages)
        self.identify_sink_pages()
        self.count_out_links()

    def get_page_rank(self):
        for i in self.all_pages:
            self.pr[i] = 1 / self.num_ol
        change = 1
        iterations = 0
        convergence_threshold = 0.0001
        while change > convergence_threshold:
            newPR = {}
            sinkPR = sum(self.pr[p] for p in self.sink)
            for page in self.all_pages:
                print("1")
                newPR[page] = (1 - self.damp) / self.num_ol + self.damp * sinkPR / self.num_ol
                for inlink in self.in_links[page]:
                    print('2')
                    inlink_hash =  hashlib.sha256(inlink.encode('utf-8')).hexdigest()
                    if inlink_hash in self.L and self.L[inlink_hash] > 0:
                        print('3')
                        newPR[page] += self.damp * self.pr[inlink_hash] / self.L[inlink_hash]
            # Check for convergence
            change = sum(abs(newPR[page] - self.pr[page]) for page in self.all_pages)
            self.pr = newPR
            iterations += 1
            print(f"Iteration {iterations}, Change: {change}, SinkPR: {sinkPR}")
            if iterations >= 100:
                print("Reached maximum iterations.")
                break

    def identify_sink_pages(self):
        # Pages with no out-links are considered sink pages
        self.sink = [p for p in self.all_pages if not self.out_links.get(p)]


    def count_out_links(self):
        # Count the number of out-links for each page
        for page in self.all_pages:
            self.L[page] = len(self.out_links.get(page, []))

    def print_top_500(self):
        result_file_path = "./links/page_rank_results_index.txt"
        if os.path.exists(result_file_path):
            os.remove(result_file_path)
        
        page_width = max(len(page) for page in self.pr.keys()) + 2
        rank_width = max(len(f"{rank:.16f}") for rank in self.pr.values()) + 2
        outlinks_width = max(len(f"{len(self.out_links.get(page, []))}") for page in self.pr.keys()) + 2
        inlinks_width = max(len(f"{len(self.in_links.get(page, []))}") for page in self.pr.keys()) + 2
        # Header
        header = f"{'Page'.ljust(page_width)}{'Page Rank'.ljust(rank_width)}{'No. of Outlinks'.ljust(outlinks_width)}{'No. of Inlinks'.ljust(inlinks_width)}\n"
        with open(result_file_path, "w") as f:
            f.write(header)
            for page, rank in sorted(self.pr.items(), key=lambda item: item[1], reverse=True)[:500]:
                outlinks_count = len(self.out_links.get(page, []))
                inlinks_count = len(self.in_links.get(page, []))
                line = f"{page.ljust(page_width)}{f'{rank:.16f}'.ljust(rank_width)}{str(outlinks_count).ljust(outlinks_width)}{str(inlinks_count).ljust(inlinks_width)}\n"
                f.write(line)

    def read_in_links(self):
        with open("./links/in_link.txt", "r") as f:
            for line in f.readlines():
                new_line = line.replace(" \n", "")
                new_line = new_line.replace("\n", "")
                new_line = new_line.split(" ")
                if len(new_line) == 1:
                    self.in_links[new_line[0]] = []
                else:
                    self.in_links[new_line[0]] = new_line[1:]
        #first_item = next(iter(self.in_links.items()))
        #print(f"First item in self.in_links: {first_item}") #First item in self.in_links: ('5a293c60e23e5e23c411a5c4146c13939dd93714447e3900a14c005fc7cbe9c5', ['http://en.wikipedia.org/wiki/Fall_of_the_Iron_Curtain', 'http://en.wikipedia.org/wiki/Warsaw_Pact_invasion_of_Czechoslovakia', 'http://en.wikipedia.org/wiki/Anti-communism', 'http://en.wikipedia.org/wiki/Prague', 'http://en.wikipedia.org/wiki/Prague_Spring', 'http://en.wikipedia.org/wiki/Revolutions_of_1989', 'http://en.wikipedia.org/wiki/Velvet_Revolution', 'http://en.wikipedia.org/wiki/Alexander_Dub%C4%8Dek', 'http://en.wikipedia.org/wiki/Anti-communist'])

    def read_out_links(self):
        with open("./links/out_link.txt", "r", encoding="utf-8") as f:
            for line in f.readlines():
                new_line = line.replace(" \n", "")
                new_line = new_line.replace("\n", "")
                new_line = new_line.split(" ")
                if len(new_line) == 1:
                    self.out_links[new_line[0]] = []
                else:
                    self.out_links[new_line[0]] = new_line[1:]
        #first_item = next(iter(self.out_links.items()))
        #print(f"First item in self.out_links: {first_item}")



pr = PageRank()

pr.get_page_rank()
pr.print_top_500()