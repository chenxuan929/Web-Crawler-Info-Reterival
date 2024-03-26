from urllib.parse import urlparse, unquote
import requests
from bs4 import BeautifulSoup
import pickle
import hashlib



def get_keywords_from_title(url):
    try:
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.content, 'html.parser')
        title = soup.find('title').text if soup.find('title') else ''
        keywords = title.lower().split()
        return keywords
    except requests.RequestException as e:
        print(f"Failed to fetch URL {url}: {e}")
        return []

def extract_keywords_from_url(url):
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.split('/')
    keywords = [unquote(segment.replace('_', ' ')).lower() for segment in path_segments if segment]
    return keywords

def get_meta_keywords(url):
    try:
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.content, 'html.parser')
        keywords = []
        if soup.find("meta", attrs={"name": "keywords"}):
            content = soup.find("meta", attrs={"name": "keywords"})['content']
            keywords += [keyword.strip().lower() for keyword in content.split(',')]
        return keywords
    except requests.RequestException as e:
        print(f"Failed to fetch URL {url}: {e}")
        return []

def extract_domain(url):
    domain = urlparse(url).netloc
    return domain

def get_in_out_links(file_path, key):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                current_key, val = line.strip().split(': ', 1)
                if current_key == key:
                    in_links = val.split(', ') if val else []
                    print(f"In-Links for '{key}': {in_links}")
                    return in_links
        print(f"Key '{key}' not found.")
    except FileNotFoundError:
        print(f"File not found: {file_path}")



if __name__ == "__main__":
    url = "http://en.wikipedia.org/wiki/Sino-Soviet_split"
    url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
    print(url_hash)
    #get_in_out_links('./in_links.txt', "https://cs.wikipedia.org/wiki/Konfliktualismus")
    # get_in_out_links('./out_links.txt', "http://en.wikipedia.org/wiki/Sociological_theory")
#     seed_urls =  [
#     "http://en.wikipedia.org/wiki/Cold_War",
#     "http://www.historylearningsite.co.uk/coldwar.htm",
#     "http://en.wikipedia.org/wiki/Sino-Soviet_split",
#     "https://www.marxists.org/history/international/comintern/sino-soviet-split/"
# ]

#     final_keywords = set()
#     for url in seed_urls:
#         title_keywords = get_keywords_from_title(url)
#         url_keywords = extract_keywords_from_url(url)
#         meta_keywords = get_meta_keywords(url)
#         combined_keywords = set(title_keywords + url_keywords + meta_keywords)
#         final_keywords.update(combined_keywords)

#     print("Final unique keywords from all seed URLs:")
#     for keyword in final_keywords:
#         print(keyword)


#     prefer_web = ['https://projects.iq.harvard.edu/jcws/li-and-xia-summary',
#                    'https://direct.mit.edu/jcws/article-abstract/16/4/260/13504/Mao-s-China-and-the-Sino-Soviet-Split-Ideological?redirectedFrom=fulltext',
#                    'https://www.marxists.org/history/international/comintern/sino-soviet-split/index.htm'
#                    'https://www.fmprc.gov.cn/mfa_eng/ziliao_665539/3602_665543/3604_665547/200011/t20001117_697807.html'
#                    'https://www.britannica.com/topic/20th-century-international-relations-2085155/The-Sino-Soviet-split'
#                    'https://www.quora.com/Why-did-the-Sino-Soviet-split-happen'
#     ]
#     prefer_domain = []
#     for web in prefer_web:
#         prefer_domain.append(extract_domain(web))
#     print("Final prefer_domain from prefer website:")
#     print(prefer_domain)
    # with open('./Results/in_links_state.pkl', 'rb') as pkl_file:
    #     in_links_dict = pickle.load(pkl_file)
    # text_file_path = 'in_links.txt'
    # with open(text_file_path, 'w', encoding='utf-8') as txt_file:
    #     for key, value in in_links_dict.items():
    #         links_str = ', '.join(value)
    #         txt_file.write(f'{key}: {links_str}\n')

    # with open('./Results/out_links_state.pkl', 'rb') as pkl_file:
    #     in_links_dict = pickle.load(pkl_file)
    # text_file_path = 'out_links.txt'
    # with open(text_file_path, 'w', encoding='utf-8') as txt_file:
    #     for key, value in in_links_dict.items():
    #         links_str = ', '.join(value)
    #         txt_file.write(f'{key}: {links_str}\n')


   


    




