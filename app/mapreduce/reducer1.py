# app/mapreduce/reducer1.py
import sys
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_index
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""")
session.set_keyspace('search_index')

session.execute("""
    CREATE TABLE IF NOT EXISTS document_stats (
        doc_id text PRIMARY KEY,
        doc_length int
    )
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term text,
        doc_id text,
        term_freq int,
        PRIMARY KEY (term, doc_id)
    )
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        doc_freq int
    )
""")

current_term = None
posting_list = []  
doc_count = {}  

def flush_term(term, postings):
    if not postings:
        return
    df = len(postings)
    for record in postings:
        doc, freq = record.split(":")
        session.execute(
            "INSERT INTO inverted_index (term, doc_id, term_freq) VALUES (%s, %s, %s)",
            (term, doc, int(freq))
        )
    session.execute(
        "INSERT INTO vocabulary (term, doc_freq) VALUES (%s, %s)",
        (term, df)
    )

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, value = line.split("\t", 1)
    
    if key.startswith("!doc!"):
        doc_id = key[5:]
        try:
            doc_length = int(value)
        except ValueError:
            continue
        session.execute(
            "INSERT INTO document_stats (doc_id, doc_length) VALUES (%s, %s)",
            (doc_id, doc_length)
        )
        continue

    term = key
    if current_term is None:
        current_term = term

    if term != current_term:
        flush_term(current_term, posting_list)
        posting_list = []
        current_term = term

    posting_list.append(value)

if current_term is not None:
    flush_term(current_term, posting_list)
