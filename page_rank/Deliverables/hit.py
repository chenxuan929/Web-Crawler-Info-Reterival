import random
import math
import hashlib
from collections import defaultdict

class Hits:
    def __init__(self, root_set_ids):
        self.in_links = {}
        self.out_links = {}
        self.root_set_ids = root_set_ids
        self.base_set_ids = set(root_set_ids)
        # If the number of additional pages exceeds the limit (d=200), it randomly selects d pages to add
        self.expansion_limit = 200
        self.authority_scores = defaultdict(lambda: 1) #default number 1
        self.hub_scores = defaultdict(lambda: 1)
        
        self.load_in_links()
        self.load_out_links()

    def hash_url(self, url):
        # Check if the URL is already hashed (simple heuristic based on length and content)
        if len(url) == 64 and all(c in '0123456789abcdef' for c in url):
            return url
        else:
            return hashlib.sha256(url.encode('utf-8')).hexdigest()


    def expand_base_set(self, target_size=10000, max_iterations=3):
        # expands the base set by including both in-links and out-links for each page in the current set.
        # repeat 3 times max
        iteration = 0
        while len(self.base_set_ids) < target_size and iteration < max_iterations:
            iteration += 1
            print(f"Iteration {iteration} of base set expansion.")
            temp_base_set = self.base_set_ids.copy()
            for page_id in list(self.base_set_ids):
                if page_id in self.out_links:
                    additional_out_links = set(random.sample(self.out_links[page_id], 
                                                            min(len(self.out_links[page_id]), self.expansion_limit)))
                    hashed_additional_out_links = {self.hash_url(each) for each in additional_out_links}
                    temp_base_set.update(hashed_additional_out_links)
                if page_id in self.in_links:
                    additional_in_links = set(random.sample(self.in_links[page_id], 
                                                            min(len(self.in_links[page_id]), self.expansion_limit)))
                    hashed_additional_in_links = {self.hash_url(each) for each in additional_in_links}
                    temp_base_set.update(hashed_additional_in_links)
                if len(temp_base_set) >= target_size:
                    break
            self.base_set_ids = temp_base_set
            print(f"Base set expanded to {len(self.base_set_ids)} pages after iteration {iteration}.")

    def compute_hits_scores(self):
        convergence_threshold = 1e-5 # Sets the convergence threshold to 1e-
        iteration = 0 # keep track of the number of iterations 
        while True:
            iteration += 1
            norm = 0 #normalization factor
            new_authority_scores = defaultdict(lambda: 0) #store the new authority scores

            for page in self.base_set_ids: #loop each page, and also loop each in link for each page
                for in_link in self.in_links.get(page, []):
                    hashed_il = self.hash_url(in_link)
                    if hashed_il in self.base_set_ids: # if current one already in base set, update the score
                        new_authority_scores[page] += self.hub_scores[hashed_il]
                norm += new_authority_scores[page] ** 2 #also update norm
            norm = norm ** 0.5
            for page in self.base_set_ids: # loop to compute new authority scores based on in links
                self.authority_scores[page] = new_authority_scores[page] / norm if norm else 0
            norm = 0
            new_hub_scores = defaultdict(lambda: 0)
            for page in self.base_set_ids:# loop to compute new authority scores based on out links
                for out_link in self.out_links.get(page, []):
                    hashed_ol = self.hash_url(out_link)
                    if hashed_ol in self.base_set_ids:
                        new_hub_scores[page] += self.authority_scores[hashed_ol]
                norm += new_hub_scores[page] ** 2
            norm = norm ** 0.5
            for page in self.base_set_ids:
                self.hub_scores[page] = new_hub_scores[page] / norm if norm else 0
            # if desired amount of iteration is finished or new score less than convergence threhols, break the loop
            if iteration > 10 or max(abs(self.authority_scores[page] - new_authority_scores[page]) for page in self.base_set_ids) < convergence_threshold:
                break
        print(f"HITS converged after {iteration} iterations.")

    # def update_authority_scores(self):
    #     new_authority_scores = {}
    #     norm = 0
    #     for page_id in self.base_set_ids:
    #         new_authority_scores[page_id] = sum(self.hub_scores.get(in_link, 0) for in_link in self.in_links.get(page_id, []))
    #         norm += new_authority_scores[page_id] ** 2
    #     norm = math.sqrt(norm)
    #     if norm > 0:
    #         changed = any(abs(self.authority_scores[page_id] - (new_authority_scores[page_id] / norm)) > 1e-5 for page_id in self.base_set_ids)
    #         self.authority_scores = {page_id: score / norm for page_id, score in new_authority_scores.items()}
    #     else:
    #         changed = False
    #     return changed

    # def update_hub_scores(self):
    #     new_hub_scores = {}
    #     norm = 0
    #     for page_id in self.base_set_ids:
    #         new_hub_scores[page_id] = sum(self.authority_scores.get(out_link, 0) for out_link in self.out_links.get(page_id, []))
    #         norm += new_hub_scores[page_id] ** 2
    #     norm = math.sqrt(norm)
    #     if norm > 0:
    #         changed = any(abs(self.hub_scores[page_id] - (new_hub_scores[page_id] / norm)) > 1e-5 for page_id in self.base_set_ids)
    #         self.hub_scores = {page_id: score / norm for page_id, score in new_hub_scores.items()}
    #     else:
    #         changed = False
    #     return changed

    def load_in_links(self):
        try:
            with open("./links/in_link.txt", "r") as file:
                for line in file:
                    page_id, *in_links = line.strip().split()
                    self.in_links[page_id] = in_links
        except IOError as e:
            print(f"Failed to load in-links from file: {e}")

    def load_out_links(self):
        try:
            with open("./links/out_link.txt", "r") as file:
                for line in file:
                    parts = line.strip().split()
                    page_id = parts[0]
                    out_links = parts[1:]
                    self.out_links[page_id] = out_links
        except IOError as e:
            print(f"Failed to load out-links from file: {e}")

    def save_top_500(self, scores, filename):
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:500]
        with open(filename, 'w') as file:
            for page, score in sorted_scores:
                file.write(f"{page}\t{score}\n")


# def main():
#     test_root_ids = ['', '', '']
#     hits_processor = Hits(test_root_ids)
#     hits_processor.expand_base_set()
#     hits_processor.compute_hits_scores()
#     print("\nThis is output of Authority Scores: \n")
#     for page_id, score in hits_processor.authority_scores.items():
#         print(f"Page id <{page_id}>: Score<{score}>")
#     print("\nThis is the Hub Scores:\n")
#     for page_id, score in hits_processor.hub_scores.items():
#         print(f"Page id <{page_id}>: Score<{score}>")

# if __name__ == "__main__":
#     main()

