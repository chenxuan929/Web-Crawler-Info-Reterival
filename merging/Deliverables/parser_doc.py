import os
import re
import logging
from multiprocessing import Pool
from nltk.stem.snowball import SnowballStemmer

class TextProcessingAndParsing:
    def __init__(self, path, stopwords_file, if_stemm =True):
        self.stopwords = self._load_stopwords(stopwords_file)
        self.path = path
        self.if_stemm = if_stemm
        self.stemmer = SnowballStemmer('english') if if_stemm else None
        self.docId_regex = re.compile(r"<DOCNO> (.*) </DOCNO>")

    def _load_stopwords(self, file_path):
        '''
        Load the stopwords from the specific file path
        '''
        with open(file_path, 'r', encoding='utf-8') as file:
            return set(line.strip().lower() for line in file if line.strip())
    
    def _preprocess_text(self, text):
        '''
        Preprocess the text by remove the stopwords and conver to lower case
        Stem words optionally
        '''
        words = text.split()
        if self.if_stemm:
            words = [self.stemmer.stem(word.lower()) for word in words if word.lower() not in self.stopwords]
        else:
            words = [word.lower() for word in words if word.lower() not in self.stopwords]
        return ' '.join(words)
    

    def _parse_file(self, file_path):
        '''
        Parse the content of a file and extract documents
        '''
        docs = {}
        content = []
        doc_id = None
        inside_t = False
        try:
            with open(file_path, "r", encoding="ISO-8859-1") as file:
                for line in file:
                    line = line.strip()
                    if "<DOCNO>" in line:
                        doc_id = self.docId_regex.search(line).group(1)
                    elif "<TEXT>" in line:
                        inside_t = True
                    elif "</TEXT>" in line:
                        inside_t = False
                    elif inside_t:
                        tokens = self._preprocess_text(line)
                        content.append(tokens)
                    elif "</DOC>" in line and doc_id:
                        docs[doc_id] = ' '.join(content)
                        content = []  
        except IOError as err:
            logging.error(f"Can not parse file {file_path}: {err}")
        return docs
    

    def process_documents(self, n_workers=os.cpu_count()):
        '''
        Process documents using multiprocessing to optimize
        '''
        # Generate file paths for all files in the specified directory
        file_paths = [os.path.join(self.path, f) for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]
        pool = Pool(processes=n_workers)
        all_docs = {} # Dictionary to store all parsed documents
        for result in pool.imap_unordered(self._parse_file, file_paths):
            all_docs.update(result)
        # Close the pool to prevent any more tasks from being submitted
        pool.close()
        pool.join() # Block until all worker processes are finished
        return all_docs




# processor = TextProcessingAndParsing("/path/to/documents", "/path/to/stopwords.txt", use_stemming=True)
# documents = processor.process_documents(n_workers=4import os
