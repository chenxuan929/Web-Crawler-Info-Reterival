import re, string

class Tokenizing:
    def __init__(self):
        self.termID_map = {}
        self.idTerm_map = {}  # Reverse map: Maps IDs to terms
        self.nextTermID = 1
        self.pattern = re.compile(r"\b\w+(?:\.\w+)*\b")
        self.trans = str.maketrans(string.punctuation.replace('.', ''), ' ' * (len(string.punctuation) - 1))
    

    def _process_text(self, text):
        '''
        Remove punctuation and converts it to lowercase
        '''
        return text.translate(self.trans).lower()
    
    def _assign_term_id(self, token):
        '''
        Assigns a unique ID to a token if it is new, or returns its ID
        '''
        if token not in self.termID_map:
            self.termID_map[token] = self.nextTermID
            self.idTerm_map[self.nextTermID] = token
            self.nextTermID += 1
        return self.termID_map[token]

    def tokenize(self, doc_id, text):
        '''
        Tokenize a given text,
        returns a list of (term_id, doc_id, position) tuples
        '''
        if not isinstance(text, str):
            raise ValueError("Tokenizing failed, The input text is not a string.")

        processed_t = self._process_text(text)
        tokens = self.pattern.findall(processed_t)
        tuples = [(self._assign_term_id(token), doc_id, pos+1) for pos, token in enumerate(tokens)]
        return tuples




# if __name__ == "__main__":
#     tokenizer = Tokenizing()
#     doc_id = 20
#     text = "The car was in the car wash."
#     token_tuples = tokenizer.tokenize(doc_id, text)
#     print(token_tuples)
#     # output correct: [(1, 20, 1), (2, 20, 2), (3, 20, 3), (4, 20, 4), (1, 20, 5), (2, 20, 6), (5, 20, 7)]

