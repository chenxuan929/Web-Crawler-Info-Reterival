from collections import defaultdict
import os
from queue import PriorityQueue
import re
from langdetect import detect
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
import time
import pickle
import logging
import hashlib

logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s %(message)s')

class FrontierItem:
    preferred_domains = {'projects.iq.harvard.edu': 5, 'direct.mit.edu': 5, 'www.marxists.org': 10}

    def __init__(self, url, in_link_count=1, wave_number=0, timestamp=None, domain='', keyword_match=0, is_seed=False, raw_html=None):
        self.url = url
        self.wave_number = wave_number
        self.timestamp = timestamp or time.time()
        self.domain = domain or urlparse(url).netloc
        self.keyword_match = keyword_match
        self.score = 0
        self.is_seed = is_seed
        self.is_valid = True
        self.in_link_count = in_link_count
        self.update_score()
        self.raw_html = raw_html


    def get_score(self):
        score = (self.in_link_count * 2) + (self.keyword_match * 3)
        if self.is_seed:
            score += 50
        if self.domain in self.preferred_domains:
            score += self.preferred_domains[self.domain]
        return score
    
    def update_score(self):
        self.score = self.get_score()
    
    def __lt__(self, other):
        if self.get_score() == other.get_score():
            return self.timestamp < other.timestamp
        return self.get_score() > other.get_score()
    
def calculate_keyword_matches(text):
    keywords = ['sino-soviet', 'split', 'cold', 'international', 'international communist movement', 'prc', 'ussr', 'war', 'comintern', 'split', 'history']
    match_count = sum(keyword in text for keyword in keywords)
    return match_count

class WebCrawler:
    def __init__(self, seed_urls):
        self.frontier = PriorityQueue()
        self.visited_urls = set()
        self.crawled_count = 0
        self.last_request_time = {}
        self.robot_parsers = {}
        self.url_to_frontier_item = defaultdict(lambda: FrontierItem(url='', in_link_count=0))
        self.in_links_dict = defaultdict(set)
        self.out_links_dict = defaultdict(set)
        for url in seed_urls:
            self.add_url_to_frontier(url, 0, is_seed=True)
        self.load_state()

        self.url_blacklist = set([
            ".jpg", ".svg", ".png", ".pdf", ".gif",
            "youtube", "edit", "footer", "sidebar", "cite",
            "special", "mailto", "books.google", "tel:",
            "javascript", "www.vatican.va", ".ogv", "amazon",
            ".webm", ".mp3", ".mp4", ".avi", ".mov",
            ".zip", ".bin", ".dmg", ".pptx", ".xls", ".ppt", "xlsx"
        ])
        
    def add_url_to_frontier(self, url, wave_number, keyword_match=0, is_seed=False):
        existing_item = self.url_to_frontier_item.get(url)
        if existing_item:
            existing_item.is_valid = False 
        in_link_count = existing_item.in_link_count + 1 if existing_item else 1
        new_item = FrontierItem(url, in_link_count, wave_number, keyword_match, is_seed=is_seed)
        new_item.update_score()
        self.url_to_frontier_item[url] = new_item
        self.frontier.put((-new_item.score, new_item))

    def process_links_found(self, parent_item, soup, parent_url): 
        """Process the links found in URLs"""
        wave_number = parent_item.wave_number + 1
        for link in soup.find_all('a', href=True):
            absolute_link = self.canonicalize_url(urljoin(parent_item.url, link['href']))
            if self.is_valid_url(absolute_link) and absolute_link not in self.visited_urls and not any(keyword in absolute_link for keyword in self.url_blacklist):
                keyword_match = calculate_keyword_matches(link.text)
                self.add_url_to_frontier(absolute_link, wave_number, keyword_match)
                self.in_links_dict[absolute_link].add(parent_url)
                self.out_links_dict[parent_url].add(absolute_link)

    def politeness_policy(self, domain):
        current_time = time.time()
        if domain in self.last_request_time:
            time_since_last_request = current_time - self.last_request_time[domain]
            if time_since_last_request < 1:
                time.sleep(1 - time_since_last_request)
        self.last_request_time[domain] = time.time()
        
    def robot_parser(self, url):
        """Ensure robots.txt is fetched with a specific timeout. Setup and return a RobotFileParser for the given URL."""
        parsed_url = urlparse(url)
        domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
        if domain not in self.robot_parsers:
            robots_url = f"{domain}/robots.txt"
            parser = RobotFileParser(robots_url)
            try:
                parser.read()
            except Exception as e:
                logging.error(f"Failed to fetch or parse {robots_url}: {e}")
            self.robot_parsers[domain] = parser
        return self.robot_parsers[domain]
    
    def can_fetch(self, url):
        """Check if URL can be fetched according to robots.txt using domain's RobotFileParser"""
        parser = self.robot_parser(url)
        return parser.can_fetch("*", url)
    
    def crawl(self):
        """Main craw method. Continues until a specific number of URLs processed. Loop throught URLs in frontier."""
        while not self.frontier.empty() and self.crawled_count < 40000:  # Example limit for testing
            _, current_item = self.frontier.get()
            current_url = current_item.url
            if current_url in self.visited_urls or any(keyword in current_url for keyword in self.url_blacklist):
                continue
            self.visited_urls.add(current_url)
            if self.can_fetch(current_url):
                self.process_url(current_item)
            if self.crawled_count % 200 == 0:
                self.save_state()
            self.crawled_count += 1
            #logging.info(f"Crawled {self.crawled_count} Scored {current_item.score}: {current_url}")
        
    def extract_text(self, soup):
        for script_or_style in soup(["script", "style"]):
            script_or_style.decompose()
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        return text
    
    def write_ap89_doc(self, docno, title, text):
        filename_hash = hashlib.sha256(docno.encode('utf-8')).hexdigest()
        directory_path = 'ap89'
        file_path = os.path.join(directory_path, f"{filename_hash}.txt")
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        
        document_formated = f"""<DOC>
    <DOCNO>{docno}</DOCNO>
    <HEAD>{title}</HEAD>
    <TEXT>{text}</TEXT>
    </DOC>
    """
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(document_formated)

    # def write_raw_html(self, url, raw_html):
    #     try:
    #         filename_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
    #         directory_path = 'raw_html'
    #         file_path = os.path.join(directory_path, f"{filename_hash}.html")
    #         os.makedirs(directory_path, exist_ok=True)
    #         with open(file_path, 'w', encoding='utf-8') as f:
    #             f.write(raw_html)
    #     except Exception as e:
    #         logging.error(f"Error writing raw HTML for URL {url}: {e}")

    def process_url(self, frontier_item):
        """Process the content of URL, checked for visited, robots.txt, canonicalize, politeness."""
        url = frontier_item.url
        domain = urlparse(url).netloc
        self.politeness_policy(domain)
        try:
            response = requests.get(url, timeout=5)
            if not response.content:
                logging.info(f"No content returned for URL: {url}")
                return
            final_url = response.url
            if response.status_code == 200 and 'text/html' in response.headers.get('Content-Type', ''):
                if any(keyword in url for keyword in self.url_blacklist):
                    logging.info(f"Skipping blacklisted url: {url}")
                    return
                content = response.content.decode('utf-8', 'ignore')
                # self.raw_html = response.text
                lang = detect(content)
                if lang != 'en':
                    logging.info(f"Skipping non-English page: {url}")
                    return
                soup = BeautifulSoup(response.content, 'html.parser')
                title = soup.find('title').text if soup.find('title') else 'No title'
                text = self.extract_text(soup)
                
                self.write_ap89_doc(final_url, title, text)
                # self.write_raw_html(final_url, self.raw_html)
                self.process_links_found(frontier_item, soup, final_url)
        except requests.RequestException as e:
            logging.error(f"Failed to fetch URL {url}: {e}")
    
    def index_document(self, document):
        self.es.index(index="web_crawl", document=document)

    
    def is_valid_url(self, url):
        """Check if a URL is valid based on its scheme and netloc, ensure it can be user as a web resource"""
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)
        
    def canonicalize_url(self, url):
        """Apply URL canonicalization rules. Avoid process same resource multi times uder different URLs."""
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme.lower()
        netloc = parsed_url.netloc.lower()
        if netloc.startswith('www.'):
            netloc = netloc[4:]
        if scheme == "http":
            netloc = netloc.replace(":80", "")
        elif scheme == "https":
            netloc = netloc.replace(":443", "")
        path = re.sub(r"/+", "/", parsed_url.path).rstrip('/')
        query = parsed_url.query #
        canonical_url = urlunparse((scheme, netloc, path, '', query,''))
        return canonical_url

    def save_links_to_txt(self):
        self.save_dict_to_txt(self.in_links_dict, 'in_links.txt')
        self.save_dict_to_txt(self.out_links_dict, 'out_links.txt')

    def save_dict_to_txt(self, dict_data, file_name):
        with open(file_name, 'w') as file:
            for key, value in dict_data.items():
                file.write(f'{key}: {value}\n')

    # These two functions help saving the state of the crawler, allow the crawler process to resume from where it paused before.
    def save_state(self):
        frontier_data = [(item.score, item.url, item.in_link_count, item.wave_number, item.keyword_match, item.timestamp, item.domain) for _, item in self.frontier.queue]
        with open('crawler_state.pkl', 'wb') as f:
            pickle.dump((frontier_data, self.visited_urls, self.crawled_count), f)
        self.save_links_state() # Save the in-links and out-links state
        self.save_links_to_txt()
        logging.info(f"Crawled {self.crawled_count}")
        logging.info("Crawler state and in&out link state saved.")

    def load_state(self):
        logging.info("Attempting to load previous state...")
        try:
            with open('crawler_state.pkl', 'rb') as f:
                frontier_data, self.visited_urls, self.crawled_count = pickle.load(f)
                
                self.frontier = PriorityQueue()
                for data in frontier_data:
                    score, url, in_link_count, wave_number, keyword_match, timestamp, domain = data
                    item = FrontierItem(url, in_link_count, wave_number, timestamp, domain, keyword_match)
                    item.score = score
                    self.frontier.put((-item.score, item))
                    self.url_to_frontier_item[url] = item
                
                logging.info("Previous crawler state loaded, starting from the latest state.")
        except FileNotFoundError:
            logging.info("No previous state found. Starting a fresh crawler.")

    def get_in_links(self, url):
        """Retrieve the set of URLs that link to the given URL."""
        return self.in_links_dict.get(url, set())
    
    def get_out_links(self, url):
        """Retrieve the set of URLs that the given URL links out to."""
        return self.out_links_dict.get(url, set())
    
    def save_links_state(self):
        with open('in_links_state.pkl', 'wb') as f:
            pickle.dump(dict(self.in_links_dict), f)
        with open('out_links_state.pkl', 'wb') as f:
            pickle.dump(dict(self.out_links_dict), f)
        logging.info("In-links and Out-links state saved.")


if __name__ == "__main__":
    seed_urls =  [
    "http://en.wikipedia.org/wiki/Cold_War",
    "http://www.historylearningsite.co.uk/coldwar.htm",
    "http://en.wikipedia.org/wiki/Sino-Soviet_split",
    "https://www.marxists.org/history/international/comintern/sino-soviet-split/"
]

    crawler = WebCrawler(seed_urls)
    crawler.crawl()

    # for each in seed_urls:
    #     print("In link set: /n")
    #     print(crawler.get_in_links(each))
    #     print("Out link set: /n")
    #     print(crawler.get_out_links(each))
