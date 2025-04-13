from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_keyspace')

rows = session.execute('SELECT term, df, n, avg_doc_length FROM statistics')

top10 = sorted(rows, key=lambda r: r.avg_doc_length, reverse=True)[:10]

for row in top10:
    print(row.term, row.avg_doc_length)
