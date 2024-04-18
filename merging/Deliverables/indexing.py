from collections import defaultdict
from tokenizing import Tokenizing
from parser_doc import TextProcessingAndParsing
import os, json, gzip
import encoding
import math
import re


def postingsDefault():
    return {'TF': 0, 'POS': []}

class Indexer:
    def __init__(self, if_stemm, outputIdx_file='idx_output', maxDocs=None, chunk_size=1000): 
        # limiting the number of postings per term in memory to 1000
        self.tokenizing = Tokenizing()
        self.chunk_size = chunk_size
        # Define output file path
        self.outputIdx_file = f"{outputIdx_file}/{'doStem' if if_stemm else 'noStem'}"
        self.mergedIdx_file = os.path.join(self.outputIdx_file, "mergedIdx")
        # Define structure
        self.partialIdx = defaultdict(lambda: {'DF': 0, 'CF': 0, 'Postings': defaultdict(postingsDefault)})
        self.total_terms = 0 # total CF
        self.total_docs = 0  # total docs
        self.total_docs_length = 0 # total docs' length
        self.avg_doc_length = 254.93791775904012 # average length of docs
        self.file_nums = 0 # number of file
        self.maxDocs = maxDocs
        self.doc_ids = {}
        self.doc_len_map = {}
        self.df_map = {}
        
        self.cf_map = {}
        self.postings_map = defaultdict(lambda: {'DF': 0, 'CF': 0, 'Postings': defaultdict(postingsDefault)})
        self.id_to_docno = {}
        
        
        if not os.path.exists(self.outputIdx_file):
            os.makedirs(self.outputIdx_file)

    def get_documents(self, term_id):
        """
        Retrieve a list of document IDs that contain the given term_id.
        """
        # Check if term_id exists in the inverted index
        if term_id in self.inverted_index:
            # Return the list of document IDs for this term
            return list(self.inverted_index[term_id].keys())
        else:
            # Return an empty list if the term_id is not found
            return []
    

    def index_doc(self, doc_name, text):
        self.total_docs += 1
        self.total_docs_length += len(text.split())
        doc_id = self.doc_ids.setdefault(doc_name, len(self.doc_ids) + 1)
        self.doc_len_map.setdefault(doc_id, len(text.split()))

        tokenize_tuples = self.tokenizing.tokenize(doc_id, text)
        for term_id, d_i, position in tokenize_tuples:
            term_meta = self.partialIdx[term_id]
            postings = term_meta['Postings']
            if len(postings) > 1000:
                break
            if doc_id not in postings:
                term_meta['DF'] += 1

            postings[doc_id]['TF'] += 1
            postings[doc_id]['POS'].append(position)
            term_meta['CF'] += 1

            
        if self.total_docs % self.chunk_size == 0 or self.total_docs == self.maxDocs:
            self.writeDisk_partialIdx()
        self.total_terms += len(tokenize_tuples)

    
    def writeDisk_partialIdx(self):
        '''
        Write the partial index to disk
        '''
        partialIndex_path = os.path.join(self.outputIdx_file, f'partialIndex_{self.file_nums + 1}.txt')
        with open(partialIndex_path, 'w') as file:
            for term_id, data in self.partialIdx.items():
                file.write(f'{term_id}: {{"DF": {data["DF"]}, "CF": {data["CF"]}, "Postings": {json.dumps(data["Postings"])}}}\n')

        self.file_nums += 1
        self.partialIdx = defaultdict(lambda: {'DF': 0, 'CF': 0, 'Postings': defaultdict(postingsDefault)})

    def writeDisk_meta(self):
        '''
        Write the metadata of docs to disk
        '''
        if not os.path.exists(self.mergedIdx_file):
            os.makedirs(self.mergedIdx_file)

        docIds_path = os.path.join(self.mergedIdx_file, f'Store_docIds.txt')
        docsMeta_path = os.path.join(self.mergedIdx_file, f'Store_docsMeta.txt')
        termIds_path = os.path.join(self.mergedIdx_file, f'Store_termIds.txt')
        doc_len_path = os.path.join(self.mergedIdx_file, f'Store_doclen.txt')

        self.avg_doc_length = self.total_docs_length / self.total_docs 

        with open(docIds_path, 'w') as f:
            for doc_name, doc_id in self.doc_ids.items():
                f.write(f'Document name: {doc_name}, Document ID: {doc_id}\n')
                self.id_to_docno[doc_id] = doc_name

        with open(docsMeta_path, 'w') as f:
            f.write(f'Vocabulary Size: {len(self.tokenizing.termID_map)},\n Total CF: {self.total_terms},\n Average length of Documents: {self.avg_doc_length}')

        with open(termIds_path, 'w') as f:
            for term_name, term_id in self.tokenizing.termID_map.items():
                f.write(f'Term name: {term_name}, Term ID: {term_id}\n')

        with open(doc_len_path, 'w') as f:
            for doc_id, doc_length in self.doc_len_map.items():
                f.write(f'Document ID: {doc_id}, Document length: {doc_length}\n')
    


    # def merge(self, if_compress):
    #     partial_indexes = [os.path.join(self.outputIdx_file, n) for n in os.listdir(self.outputIdx_file) if os.path.isfile(os.path.join(self.outputIdx_file, n)) and n.startswith("partialIndex_")]

    #     for filename in partial_indexes:
    #         with open(filename, 'r') as file:
    #             for line in file:
    #                 try:
    #                     term_id, json_string = line.split(':', 1)
    #                     term_id = term_id.strip()
    #                     json_data = json.loads(json_string.strip())

    #                     self.df_map[term_id] = self.df_map.get(term_id, 0) + json_data['DF']
    #                     self.cf_map[term_id] = self.cf_map.get(term_id, 0) + json_data['CF']
                        
    #                     if term_id not in self.postings_map:
    #                         self.postings_map[term_id] = json_data['Postings']
    #                     else:
    #                         for doc_id, posting_data in json_data['Postings'].items():
    #                             if doc_id in self.postings_map[term_id]:
    #                                 existing_posting = self.postings_map[term_id][doc_id]
    #                                 existing_posting['TF'] += posting_data['TF']
    #                                 existing_posting['POS'].extend(posting_data['POS'])
    #                             else:
    #                                 self.postings_map[term_id][doc_id] = posting_data
                           
    #                 except json.JSONDecodeError as e:
    #                     print(f"Error with file {filename} in line: {line}\n")
    #                     print(f"Error message: {e}")
            
            
    #         os.remove(filename)

    #     # Sort postings by document frequency
    #     sorted_postings = {}
    #     for term_id, postings in self.postings_map.items():
    #         sorted_docs = sorted(postings.items(), key=lambda x: x[1]['TF'], reverse=True)[:1000]
    #         sorted_postings[term_id] = dict(sorted_docs)

    #     # Write merged index to disk
    #     if if_compress == True:
    #         with gzip.open(f'{self.outputIdx_file}/mergedIndex_compressed.txt.gz', 'wt', encoding='utf-8') as merged_index_file:
    #             for term_id, postings in sorted_postings.items():
    #                 merged_index_file.write(f'{term_id}: {{"DF": {self.df_map[term_id]}, "CF": {self.cf_map[term_id]}, "Postings": {json.dumps(postings)}}}\n')
    #     else:
    #         with open(f'{self.outputIdx_file}/mergedIndex.txt', 'w') as merged_index_file:
    #             for term_id, postings in sorted_postings.items():
    #                 merged_index_file.write(f'{term_id}: {{"DF": {self.df_map[term_id]}, "CF": {self.cf_map[term_id]}, "Postings": {json.dumps(postings)}}}\n')

    #     return self.postings_map


    def merge(self, if_compress):
        '''
        Merge with encoding to optimize
        '''
        partial_indexes = [os.path.join(self.outputIdx_file, n) for n in os.listdir(self.outputIdx_file) if os.path.isfile(os.path.join(self.outputIdx_file, n)) and n.startswith("partialIndex_")]

        df_map = {}
        cf_map = {}
        

        for filename in partial_indexes:
            with open(filename, 'r') as file:
                for line in file:
                    try:
                        term_id, json_string = line.split(':', 1)
                        term_id = term_id.strip()
                        json_data = json.loads(json_string.strip())

                        df_map[term_id] = df_map.get(term_id, 0) + json_data['DF']
                        cf_map[term_id] = cf_map.get(term_id, 0) + json_data['CF']
                    
                        if term_id not in self.postings_map:
                            self.postings_map[term_id] = json_data['Postings']
                        else:
                            for doc_id, posting_data in json_data['Postings'].items():
                                if doc_id in self.postings_map[term_id]:
                                    existing_posting = self.postings_map[term_id][doc_id]
                                    existing_posting['TF'] += posting_data['TF']
                                    existing_posting['POS'].extend(posting_data['POS'])
                                else:
                                    self.postings_map[term_id][doc_id] = posting_data
                       
                    except json.JSONDecodeError as e:
                        print(f"Error with file {filename} in line: {line}\n")
                        print(f"Error message: {e}")
        
        
            os.remove(filename)

        # Encoding document IDs and TFs for all term postings
        for term_id, postings in self.postings_map.items():
            doc_ids = [int(doc_id) for doc_id in postings.keys()]
            tfs = [postings[doc_id]['TF'] for doc_id in postings.keys()]
            # Apply delta encoding and then VB encode the list of document IDs and TFs
            encoded_doc_ids = encoding.vb_encode_list(encoding.delta_encode(doc_ids))
            encoded_tfs = encoding.vb_encode_list(tfs)
            # Replace postings with encoded data
            self.postings_map[term_id] = {'EncodedDocIDs': encoded_doc_ids, 'EncodedTFs': encoded_tfs}

        # Write to disk based on compression flag
        if if_compress == True:
            with gzip.open(f'{self.outputIdx_file}/mergedIndex_compressed.txt.gz', 'wt', encoding='utf-8') as merged_index_file:
                for term_id, encoded_postings in self.postings_map.items():
                    merged_index_file.write(f'{term_id}:{{"DF":{df_map[term_id]},"CF":{cf_map[term_id]},"Postings":{json.dumps(encoded_postings)}}}\n')
        else:
            with open(f'{self.outputIdx_file}/mergedIndex.txt', 'w') as merged_index_file:
                for term_id, encoded_postings in self.postings_map.items():
                    merged_index_file.write(f'{term_id}:{{"DF":{df_map[term_id]},"CF":{cf_map[term_id]},"Postings":{json.dumps(encoded_postings)}}}\n')








    def get_term_frequency(self, term_id, doc_id):
        """
        Retrieve the term frequency (TF) of a term in a given document.
        """
        if term_id in self.postings_map:
            if doc_id in self.postings_map[term_id]:
                return self.postings_map[term_id][doc_id]['TF']
        return 0





   
         

