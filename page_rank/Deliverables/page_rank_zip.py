# import zipfile
# from page_rank import PageRankCalculate

# class Rank_for_Zip:
#     def __init__(self):
#         self.page_in_links = {}
#         self.page_out_links = {}

#     def rank_zip(self, zip_filepath):
#         """Load and process in-links from a zip file."""
#         with zipfile.ZipFile(zip_filepath, 'r') as z:
#             with z.open(z.namelist()[0]) as f:
#                 for line in f:
#                     parts = line.decode().strip().split()
#                     page = parts[0]
#                     in_links = parts[1:]
#                     self.page_in_links[page] = in_links
#                     for in_link in in_links:
#                         if in_link not in self.page_out_links:
#                             self.page_out_links[in_link] = []
#                         self.page_out_links[in_link].append(page)
#         self.calculate_page_rank()

#     def calculate_page_rank(self):
#         calculator = PageRankCalculate()
#         calculator.page_in_links = self.page_in_links
#         calculator.calculate_page_rank(From_zip=True)
#         #self.page_rank_scores = calculator.page_rank_scores


# if __name__ == "__main__":
#     zip_loader = Rank_for_Zip()
#     zip_loader.load_data_from_zip("./Resources/wt2g_inlinks.txt.zip")
import numpy as np
import os

class PageRank:

    def __init__(self, damping_factor=0.85):
        self.in_links = {}  # In-links
        self.all_pages = []  # All pages
        self.num = 0   # Total number of pages
        self.out_links = {}  # Out-links

        self.sink = []  # Sink nodes, pages without out-links
        self.ol_count = {}  # Number of out-links for each page
        self.damp = damping_factor  # Damping factor
        self.pr = {}  # PageRank of each page

        self.initialize()

    def initialize(self):
        self.read_in_links()
        self.calculate_out_links()
        self.all_pages = list(self.in_links.keys())
        self.num = len(self.all_pages)
        self.get_sink_page()
        self.get_count_ol()

    def get_page_rank(self):
        for i in self.all_pages:
            self.pr[i] = 1 / self.num #iterate all pages and initializes the pagerank score to 1/num
        change = 1 #use this to track if there is any change of score later
        iterations = 0 #control the loop times
        convergence_threshold = 0.0001 #convergence threshold for the algorithm
        while change > convergence_threshold:
            sinkPR = sum(self.pr[p] for p in self.sink) #sum of PageRank scores for sink nodes
            newPR = {} #store updated PageRank scores
            for p in self.all_pages: # loop all pages to calculate the new pr score
                newPR[p] = (1 - self.damp) / self.num + self.damp * sinkPR / self.num
                for q in self.in_links[p]: # loop in link page
                    if q in self.ol_count and self.ol_count[q] != 0: # check if current one have out link
                        newPR[p] += self.damp * self.pr[q] / self.ol_count[q] # if it has, update the score
            change = sum(abs(newPR[p] - self.pr[p]) for p in self.all_pages) #check the change
            self.pr = newPR #update
            iterations += 1
            if iterations >= 100: #finish
                print("Reached maximum iterations.")
                break
        print("PageRank converged.")

    def get_sink_page(self):
        #if page do not have out link -> sink page
        self.sink = [p for p in self.all_pages if p not in self.out_links or len(self.out_links[p]) == 0]

    def get_count_ol(self):
        for p in self.all_pages:
            if p in self.out_links:
                self.ol_count[p] = len(self.out_links[p])
            else:
                self.ol_count[p] = 0

    def print_top_500(self):
        result_file_path = "./links/page_rank_results_zip.txt"
        if os.path.exists(result_file_path):
            os.remove(result_file_path)
        #final = sorted(self.pr.items(), key=lambda item: item[1], reverse=True)[:500]
        
        page_width = max(len(page) for page in self.pr.keys()) + 2  # Add some padding
        rank_width = max(len(f"{rank:.16f}") for rank in self.pr.values()) + 2
        outlinks_width = max(len(f"{len(self.out_links.get(page, []))}") for page in self.pr.keys()) + 2
        inlinks_width = max(len(f"{len(self.in_links.get(page, []))}") for page in self.pr.keys()) + 2
        header = f"{'Page'.ljust(page_width)}{'Page Rank'.ljust(rank_width)}{'No. of Outlinks'.ljust(outlinks_width)}{'No. of Inlinks'.ljust(inlinks_width)}\n"
        with open(result_file_path, "w") as f:
            f.write(header)
            for page, rank in sorted(self.pr.items(), key=lambda item: item[1], reverse=True)[:500]:
                outlinks_count = len(self.out_links.get(page, []))
                inlinks_count = len(self.in_links.get(page, []))
                line = f"{page.ljust(page_width)}{f'{rank:.16f}'.ljust(rank_width)}{str(outlinks_count).ljust(outlinks_width)}{str(inlinks_count).ljust(inlinks_width)}\n"
                f.write(line)

    def read_in_links(self):
        file_path = "./Resources/wt2g_inlinks.txt"
        with open(file_path, "r") as f:
            for line in f:
                parts = line.strip().split()
                target, sources = parts[0], parts[1:]
                self.in_links[target] = sources

    def calculate_out_links(self):
        for page, inlinks in self.in_links.items():
            for inlink in inlinks:
                if inlink not in self.out_links:
                    self.out_links[inlink] = []
                self.out_links[inlink].append(page)

pr = PageRank()
pr.get_page_rank()
pr.print_top_500()
