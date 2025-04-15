# from cassandra.cluster import Cluster


# print("hello app")

# # Connects to the cassandra server
# cluster = Cluster(['cassandra-server'])

# session = cluster.connect()

# # displays the available keyspaces
# rows = session.execute('DESC keyspaces')
# for row in rows:
#     print(row)

from cassandra.cluster import Cluster



cluster = Cluster(['cassandra'])
session = cluster.connect()
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine 
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace('search_engine')

session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term TEXT,
        doc_id TEXT,
        PRIMARY KEY (term, doc_id)
    )
""")

with open('index_output.txt') as f:
    for line in f:
        term, doc_ids = line.strip().split("\t")
        for doc_id in doc_ids.split(","):
            session.execute("INSERT INTO inverted_index (term, doc_id) VALUES (%s, %s)", (term, doc_id))
