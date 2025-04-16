# app/query.py
import sys
import re
import math
from pyspark import SparkContext
from cassandra.cluster import Cluster

if len(sys.argv) < 2:
    print("Usage: query.py <query string>")
    sys.exit(1)

query_str = sys.argv[1]
tokens = re.findall(r'\w+', query_str.lower())

sc = SparkContext(appName="BM25_Ranker")

cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')


doc_stats = session.execute("SELECT count(*) FROM document_stats")
total_docs = doc_stats.one()[0] if doc_stats.one() is not None else 1


all_docs = session.execute("SELECT doc_length FROM document_stats")
doc_lengths = [row.doc_length for row in all_docs]
avg_doc_length = sum(doc_lengths) / len(doc_lengths) if doc_lengths else 1.0


k1 = 1.5
b = 0.75

# For each query token, get posting list and compute partial BM25 scores.
def get_postings(term):
    query = "SELECT doc_id, term_freq FROM inverted_index WHERE term=%s"
    result = session.execute(query, (term,))
    return [(row.doc_id, row.term_freq) for row in result]


def get_doc_freq(term):
    query = "SELECT doc_freq FROM vocabulary WHERE term=%s"
    result = session.execute(query, (term,))
    row = result.one()
    return row.doc_freq if row is not None else 0

# Create an RDD of (doc_id, score) pairs for each term in the query.
rdd_list = []
for term in tokens:
    postings = get_postings(term)
    df = get_doc_freq(term)
    idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1)
    # Create an RDD from the posting list
    term_rdd = sc.parallelize(postings).map(lambda x: (x[0], (x[1], idf)))
    rdd_list.append(term_rdd)

if not rdd_list:
    print("No valid tokens found in query.")
    sys.exit(0)

# Union all RDDs and compute BM25 scores per document.
# all_scores = rdd_list[0].unionAll(rdd_list[1:]) if len(rdd_list) > 1 else rdd_list[0]
from functools import reduce
all_scores = reduce(lambda a, b: a.union(b), rdd_list)


# def score_mapper(record):
#     # record: (doc_id, (tf, idf))
#     tf, idf = record[1]
#     # Retrieve document length for this document from Cassandra.
#     row = session.execute("SELECT doc_length FROM document_stats WHERE doc_id=%s", (record[0],)).one()
#     doc_length = row.doc_length if row is not None else avg_doc_length
#     # BM25 score computation
#     numerator = tf * (k1 + 1)
#     denominator = tf + k1 * (1 - b + b * (doc_length / avg_doc_length))
#     return (record[0], idf * (numerator / denominator))

# # Sum BM25 scores for each document.
# doc_scores = all_scores.map(score_mapper).reduceByKey(lambda a, b: a + b)

def score_mapper_partition(records):
    from cassandra.cluster import Cluster
    # Create a new Cassandra connection in the executor (per partition)
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    
    # Iterate over partition records and compute BM25 score
    for record in records:
        doc_id, (tf, idf) = record
        # For each doc_id, query its document length
        row = session.execute("SELECT doc_length FROM document_stats WHERE doc_id=%s", (doc_id,)).one()
        doc_length = row.doc_length if row is not None else avg_doc_length
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * (doc_length / avg_doc_length))
        yield (doc_id, idf * (numerator / denominator))
    
    # Optionally, close the session or cluster connection here if needed
    session.shutdown()
    cluster.shutdown()

# Replace map() with mapPartitions()
doc_scores = all_scores.mapPartitions(score_mapper_partition) \
                        .reduceByKey(lambda a, b: a + b)



# Get top 10 documents by BM25 score.
top_docs = doc_scores.takeOrdered(10, key=lambda x: -x[1])

print("Top 10 documents for query:", query_str)
print(f"[DEBUG] Retrieved {len(top_docs)} documents", flush=True)

for doc_id, score in top_docs:
    # For display purposes, you can query the original document title if stored in document_stats or in the file names.
    print(f"Doc ID: {doc_id}, Score: {score:.4f}", flush=True)


sc.stop()
