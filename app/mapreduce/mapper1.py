# app/mapreduce/mapper1.py
import sys
import re

def tokenize(text):
    return re.findall(r'\w+', text.lower())

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    doc_id, doc_title, doc_text = parts[0], parts[1], parts[2]
    tokens = tokenize(doc_text)
    doc_length = len(tokens)
    

    sys.stdout.write(f"!doc!{doc_id}\t{doc_length}\n")
    
    term_freq = {}
    for token in tokens:
        term_freq[token] = term_freq.get(token, 0) + 1
    for token, count in term_freq.items():
        sys.stdout.write(f"{token}\t{doc_id}:{count}\n")
