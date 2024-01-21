from cassandra.cluster import Cluster


HOST = '127.0.0.1'
PORT = 9042
KEYSPACE = 'nba_data'

cluster = Cluster(HOST)
