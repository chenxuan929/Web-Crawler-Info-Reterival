from bs4 import BeautifulSoup
import requests, time, heapq, logging, os
from fake_useragent import UserAgent
from urllib.parse import urljoin, urlparse, urlunparse, unquote
from urllib.robotparser import RobotFileParser
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

class Frontier:
    def __init__(self, keywords):
        self.queue = []
        self.keywords = keywords  # List of keywords to prioritize
        self.lock = Lock()  # Ensure thread safety
        # Additional attributes for tracking links
        self.url_info = {}  # Stores metadata for each URL, including in-links and out-links
    
    def add_url(self, url, wave_number, anchor_text="", is_seed=False, discovered_from=None):
        relevance = self.calculate_relevance(url, anchor_text)
        priority = self.calculate_priority(is_seed, relevance, wave_number)

        # Initialize url_info entry if this is the first encounter
        if url not in self.url_info:
            self.url_info[url] = {"in_links": set(), "out_links": set(), "is_seed": is_seed}
            
        # If the URL was discovered from another page, update the link information
        if discovered_from:
            self.url_info[url]["in_links"].add(discovered_from)
            self.url_info[discovered_from]["out_links"].add(url)
            priority = self.calculate_priority(is_seed, relevance, wave_number, len(self.url_info[url]["in_links"]))
        
        # Update in-link count for priority calculation based on the number of unique in-links
        # in_link_count = len(self.url_info[url]["in_links"])
        entry = (priority, url)
        heapq.heappush(self.queue, entry)
            
    
    def calculate_relevance(self, url, anchor_text):
        relevance_score = 0
        for keyword in self.keywords:
            if keyword in url.lower() or keyword in anchor_text.lower():
                relevance_score += 1
        return relevance_score

    def calculate_priority(self, is_seed, relevance, wave_number, in_link_count=0):
        priority = -relevance - in_link_count + wave_number  # Higher relevance and in-link counts get higher priority
        if is_seed:
            priority -= 10000  # Ensure seed URLs have the highest priority
        return priority

    def get_next_url(self, last_crawled_domain=None):
        with self.lock:
            if not self.queue:
                return None
            
            # Attempt to find the next URL from a different domain
            for index, (_, url) in enumerate(self.queue):
                domain = urlparse(url).netloc
                if domain != last_crawled_domain:
                    # Found a URL from a different domain, remove and return it
                    return self.queue.pop(index)[1]
            
            # If all URLs are from the same domain or no different domain found, return the top URL
            return heapq.heappop(self.queue)[1] if self.queue else None
    
class Crawler:
    def __init__(self, seed_urls, max_documents, output_dir, keywords):
        self.frontier = Frontier(keywords)
        self.visited_urls = set()
        self.max_documents = max_documents
        self.documents_crawled = 0
        self.output_dir = output_dir
        self.last_crawled_domain = None
        self.num_threads = os.cpu_count()
        self.wavenumber = 0
        self.lock = Lock()
        self.block_list = [".gif", ".svg", ".dmg", "search", ".webm", ".mov", "sidebar", ".xls", ".ogv", "tel:", "musiclearningsite", ".gz", "www.vatican.va", "avery.wellesley.edu", "caboodle.studio", "xlsx", "special", "mailto", "solidarityeconomy", "edit", "javascript", ".mp3", "amazon", ".jpg", ".mp4", "youtube", ".pptx", ".pdf", ".bin", "video", "cite", "footer", ".avi", ".png", ".zip", "books.google", ".exe", ".rar", ".ppt", ".7z"]

        self.robots_parsers = {}
        self.user_agent = UserAgent().random
        self.session = requests.Session()  # Use session for persistent connections
        retry_strategy = Retry(
                total=2,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"],
                backoff_factor=0.5
            )
        self.session.mount('http://', HTTPAdapter(max_retries=retry_strategy))
        self.session.mount('https://', HTTPAdapter(max_retries=retry_strategy))
        self.session.headers.update({'User-Agent': self.user_agent})
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        if not os.path.exists(output_dir + "/documents"):
            os.makedirs(output_dir + "/documents")
        
        if not os.path.exists(output_dir + "/links"):
            os.makedirs(output_dir + "/links")

        if not os.path.exists(output_dir + "/headers"):
            os.makedirs(output_dir + "/headers")

        for url in seed_urls:
            self.frontier.add_url(url, self.wavenumber, is_seed=True)

    def start_crawling(self):
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = [executor.submit(self.crawl) for _ in range(self.num_threads)]
            for future in futures:
                future.result()  # Wait for all threads to complete

    def crawl(self):
        while True:
            current_url = self.frontier.get_next_url(self.last_crawled_domain)
            if not current_url or self.documents_crawled >= self.max_documents:
                break  # Exit if no URLs left or max documents reached
            
            domain = urlparse(current_url).netloc
            protocol = urlparse(current_url).scheme

            if not domain or current_url in self.visited_urls or domain.isdigit():
                continue

            # Check robots.txt
            if domain not in self.robots_parsers:
                self.initialize_robots_parser(protocol, domain)
            if domain in self.robots_parsers and not self.robots_parsers[domain].can_fetch(self.user_agent, current_url):
                continue
            
            if domain == self.last_crawled_domain:
                if self.robots_parsers[domain].crawl_delay(self.user_agent):
                    time.sleep(self.robots_parsers[domain].crawl_delay(self.user_agent))

            self.last_crawled_domain = domain
            logging.info(f"Processing URL: {current_url} {self.documents_crawled}/{self.max_documents} {format(self.documents_crawled / self.max_documents * 100, '.2f')}%")
            try:
                response = self.session.get(current_url, timeout=5)
                if response.status_code == 200 and self.is_html(response):
                    soup = BeautifulSoup(response.text, 'html.parser')
                    links = self.extract_links(soup, current_url)
                    curr_low_link = self.remove_http_protocol(current_url).lower()
                    for link, anchor_text in links:
                        canonical_link = self.canonicalize_url(link)
                        # Add link to frontier with discovered_from information
                        can_low_link = self.remove_http_protocol(canonical_link).lower()
                        if can_low_link not in self.visited_urls and not any(substring in can_low_link for substring in self.block_list) and can_low_link != curr_low_link:
                            self.frontier.add_url(canonical_link, self.wavenumber,anchor_text=anchor_text, discovered_from=current_url)

                    self.process_document(response, current_url)
            except requests.RequestException as e:
                print(f"Request failed for {current_url}: {e}")

            with self.lock:
                self.wavenumber += 1
                
    def initialize_robots_parser(self, protocol, domain):
        robots_url = f"{protocol}://{domain}/robots.txt"
        robots_parser = RobotFileParser()
        try:
            response = self.session.get(robots_url, timeout=5)
            response.encoding = 'utf-8'
            robots_parser.parse(response.text.splitlines())
        except requests.RequestException as e:
            print(f"Could not fetch or parse robots.txt for {robots_url}: {e}")
            # Instead of setting to None, set a default parser that allows everything
            robots_parser = RobotFileParser()
            robots_parser.parse("User-agent: *\nDisallow:")  # This effectively allows all agents
        with self.lock:
            self.robots_parsers[domain] = robots_parser

    @staticmethod
    def canonicalize_url(url):
        parsed_url = urlparse(url)

        # Check if the domain contains "web.archive.org"
        if "web.archive.org" in parsed_url.netloc:
            # Attempt to extract the original URL from the path
            # Typical archive.org URL structure: http://web.archive.org/web/{timestamp}/{original_url}
            # Splitting by '/' and checking if parts exist
            parts = parsed_url.path.split('/')
            if len(parts) > 2 and parts[1] == "web":
                # Reconstruct the original URL
                original_url = '/'.join(parts[2:])
                if original_url.startswith('http:') or original_url.startswith('https:'):
                    # Parse the original URL
                    parsed_url = urlparse(original_url)
                else:
                    # If the original URL does not start with http/https, prepend 'http://'
                    parsed_url = urlparse('http://' + original_url)

        # Normalize the netloc by removing default ports
        netloc = parsed_url.netloc.replace(':80', '').replace(':443', '')

        # Normalize path to remove duplicate slashes
        path = parsed_url.path.replace('//', '/')
        if path and not path.endswith('/'):
            path += '/'

        # Reconstruct the URL without query and fragment
        canonical_url = urlunparse((parsed_url.scheme, netloc, path, '', '', ''))

        return canonical_url.rstrip('/')

    @staticmethod
    def extract_links(soup, base_url):
        links = []
        for a in soup.find_all('a', href=True):
            url = a['href']
            if not url.startswith('http'):
                url = urljoin(base_url, a['href'])
            anchor_text = a.text
            links.append((url, anchor_text))
        return links
    
    @staticmethod
    def is_html(response):
        content_type = response.headers.get('Content-Type', '')
        return 'text/html' in content_type

    def process_document(self, response, current_url):
        soup = BeautifulSoup(response.text, 'html.parser')
       
        # Remove script and style elements
        for data in soup(["script", "style", "header"]):
            # Remove tags
            data.decompose()
        
        # Extract title
        if soup.find('title'):
            title = soup.find('title').text.strip()  
        else:
            return

        text = ' '.join(soup.stripped_strings)

        without_url = self.remove_http_protocol(current_url).lower()

        with self.lock: 
            if not text or without_url in self.visited_urls:
                return
            
            self.documents_crawled += 1
            self.visited_urls.add(without_url)

        # Format and save the document
        document_content = f"<DOC>\n<DOCNO>{current_url}</DOCNO>\n<HEAD>{title}</HEAD>\n<TEXT>{text}</TEXT>\n</DOC>"
        document_filename = f"document_{self.documents_crawled}.txt"
        document_path = os.path.join(self.output_dir + "/documents", document_filename)

        with open(document_path, 'w', encoding='utf-8') as f:
            f.write(document_content)
        
        # Store the HTTP response headers
        headers_path = os.path.join(self.output_dir + "/headers", f"headers_{self.documents_crawled}.txt")
        with open(headers_path, 'w', encoding='utf-8') as f:
            f.write(str(response.headers))

        with open(os.path.join(self.output_dir + "/links", f"links_graph_{self.documents_crawled}.txt"), "w", encoding='utf-8') as file:  # Open in append mode
            outlinks = self.frontier.url_info[current_url]["out_links"]
            inlinks = self.frontier.url_info[current_url]["in_links"]
            line = f"Inlinks:" + ' '.join(inlinks) + ", Outlinks:" + ' '.join(outlinks)
            file.write(line)
       
    def remove_http_protocol(self, url):
        return url.replace('http://', '').replace('https://', '')

def extract_keyword(url):
    path = urlparse(url).path
    decoded_path = unquote(path)
    last_part = decoded_path.split('/')[-1]
    # Handle parentheses, underscores, and hyphens
    clean_term = last_part.split('(')[0] if '(' in last_part else last_part
    words = clean_term.replace('_', ' ').replace('-', ' ').split()
    # Include terms within parentheses, if present
    if '(' in last_part and ')' in last_part:
        additional_terms = last_part.split('(')[1].split(')')[0]
        words.extend(additional_terms.replace('_', ' ').replace('-', ' ').split())
    return words


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    output_directory = './Results/'
    seed_urls = ["http://en.wikipedia.org/wiki/Cold_War",
        "http://www.historylearningsite.co.uk/coldwar.htm",
        "http://en.wikipedia.org/wiki/Sino-Soviet_split",
        "https://www.marxists.org/history/international/comintern/sino-soviet-split/"]
    keywords = [keyword.lower() for url in seed_urls for keyword in extract_keyword(url)]
    keywords = list(set(keywords))
    crawler = Crawler(seed_urls, 100000, output_directory, keywords)
    crawler.start_crawling()  # Starts the crawling process




