# # app/mapreduce/reducer1.py
# import sys
# from cassandra.cluster import Cluster

# cluster = Cluster(['cassandra-server'])
# session = cluster.connect()

# session.execute("""
#     CREATE KEYSPACE IF NOT EXISTS search_engine
#     WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
# """)
# session.set_keyspace('search_engine')

# session.execute("""
#     CREATE TABLE IF NOT EXISTS document_stats (
#         doc_id text PRIMARY KEY,
#         doc_length int
#     )
# """)
# session.execute("""
#     CREATE TABLE IF NOT EXISTS inverted_index (
#         term text,
#         doc_id text,
#         term_freq int,
#         PRIMARY KEY (term, doc_id)
#     )
# """)
# session.execute("""
#     CREATE TABLE IF NOT EXISTS vocabulary (
#         term text PRIMARY KEY,
#         doc_freq int
#     )
# """)

# current_term = None
# posting_list = []  
# doc_count = {}  

# def flush_term(term, postings):
#     if not postings:
#         return
#     df = len(postings)
#     for record in postings:
#         doc, freq = record.split(":")
#         session.execute(
#             "INSERT INTO inverted_index (term, doc_id, term_freq) VALUES (%s, %s, %s)",
#             (term, doc, int(freq))
#         )
#     session.execute(
#         "INSERT INTO vocabulary (term, doc_freq) VALUES (%s, %s)",
#         (term, df)
#     )

# for line in sys.stdin:
#     line = line.strip()
#     if not line:
#         continue
#     key, value = line.split("\t", 1)
    
#     if key.startswith("!doc!"):
#         doc_id = key[5:]
#         try:
#             doc_length = int(value)
#         except ValueError:
#             continue
#         session.execute(
#             "INSERT INTO document_stats (doc_id, doc_length) VALUES (%s, %s)",
#             (doc_id, doc_length)
#         )
#         continue

#     term = key
#     if current_term is None:
#         current_term = term

#     if term != current_term:
#         flush_term(current_term, posting_list)
#         posting_list = []
#         current_term = term

#     posting_list.append(value)

# if current_term is not None:
#     flush_term(current_term, posting_list)



# app/mapreduce/reducer1.py
import sys
import time
from cassandra.cluster import Cluster

# Retry connection to Cassandra
for attempt in range(10):
    try:
        cluster = Cluster(['cassandra-server'])  
        session = cluster.connect()
        break
    except Exception as e:
        print(f"[ERROR] Waiting for Cassandra ({attempt+1}/10): {e}", file=sys.stderr)
        time.sleep(5)
else:
    print("[FATAL] Failed to connect to Cassandra after 10 attempts.", file=sys.stderr)
    sys.exit(1)

# Ensure keyspace and tables
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""")
session.set_keyspace('search_engine')

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

def flush_term(term, postings):
    if not postings:
        return
    df = len(postings)
    print(f"[INFO] Flushing term '{term}' with DF={df}", file=sys.stderr)

    for record in postings:
        try:
            doc, freq = record.split(":")
            session.execute(
                "INSERT INTO inverted_index (term, doc_id, term_freq) VALUES (%s, %s, %s)",
                (term, doc, int(freq))
            )
        except Exception as e:
            print(f"[ERROR] inserting into inverted_index for term {term}: {e}", file=sys.stderr)

    try:
        session.execute(
            "INSERT INTO vocabulary (term, doc_freq) VALUES (%s, %s)",
            (term, df)
        )
    except Exception as e:
        print(f"[ERROR] inserting into vocabulary: {e}", file=sys.stderr)

# Main reducer logic
line_count = 0
for line in sys.stdin:
    line_count += 1
    line = line.strip()
    if not line:
        continue
    try:
        key, value = line.split("\t", 1)
    except ValueError:
        print(f"[WARN] Skipping malformed line: {line}", file=sys.stderr)
        continue

    if key.startswith("!doc!"):
        doc_id = key[5:]
        try:
            doc_length = int(value)
            session.execute(
                "INSERT INTO document_stats (doc_id, doc_length) VALUES (%s, %s)",
                (doc_id, doc_length)
            )
            print(f"[INFO] Inserted doc stats: {doc_id} (length={doc_length})", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] inserting doc stats for {doc_id}: {e}", file=sys.stderr)
        continue

    term = key
    if current_term is None:
        current_term = term

    if term != current_term:
        flush_term(current_term, posting_list)
        posting_list = []
        current_term = term

    posting_list.append(value)
print(f"[INFO] Total lines processed: {line_count}", file=sys.stderr)
# Flush last term
if current_term is not None:
    flush_term(current_term, posting_list)

print("[INFO] Reducer finished processing.", file=sys.stderr)
