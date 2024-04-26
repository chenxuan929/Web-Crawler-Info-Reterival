import os
import re
from random import shuffle
from bs4 import BeautifulSoup
from email.parser import Parser
from os import listdir
import json

class EmailParser:
    def __init__(self, data_dir="./trec07p/data/", index_file="./trec07p/full/index"):
        self.data_dir = data_dir
        self.index_file = index_file
        self.email_texts = {}
        self.labels = {}
        self.data_split = {}
        self.parser = Parser()

    def parse_emails(self):
        file_names = listdir(self.data_dir)
        for file_name in file_names:
            path = os.path.join(self.data_dir, file_name)
            with open(path, "r", encoding="ISO-8859-1") as file:
                email_data = file.read()
            email_message = self.parser.parsestr(email_data)
            email_text = self.extract_email_content(email_message)
            # self.email_texts[file_name] = BeautifulSoup(email_text, "lxml").get_text(strip=True) if email_text.strip() else ""
            cleaned_text = self.clean_html(email_text) if email_text.strip() else ""
            self.email_texts[file_name] = cleaned_text

    def clean_html(self, html_content):
        soup = BeautifulSoup(html_content, 'lxml')
        text = soup.get_text(separator=' ', strip=True)
        clean_text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        return clean_text


    def load_labels(self):
        with open(self.index_file, "r") as file:
            for line in file:
                line = line.strip()
                label, path = line.split()
                file_name = re.search(r"/(\w+\.\w+)$", path).group(1)
                self.labels[file_name] = "spam" if label.lower() == "spam" else "ham"

    def assign_data_splits(self):
        all_files = list(self.labels.keys())
        shuffle(all_files)
        split_index = int(len(all_files) * 0.8)
        for i, file_name in enumerate(all_files):
            self.data_split[file_name] = "train" if i < split_index else "test"

    def extract_email_content(self, email_message):
        if not email_message.is_multipart():
            return email_message.get_payload()
        content = ""
        for part in email_message.get_payload():
            content += self.extract_email_content(part)
        return content


    def save_processed_data(self):
        with open("./processed_emails.json", "w") as f:
            json.dump(self.email_texts, f)
        with open("./email_labels.json", "w") as f:
            json.dump(self.labels, f)
        with open("./data_splits.json", "w") as f:
            json.dump(self.data_split, f)

def main():
    email_parser = EmailParser()
    email_parser.parse_emails()
    email_parser.load_labels()
    email_parser.assign_data_splits()
    email_parser.save_processed_data()
    print("Data preparation complete. Data split into training and testing sets and saved.")

if __name__ == "__main__":
    main()
