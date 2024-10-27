from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}],
timeout=60)
if es.ping():
	print ("SI")
else:
	print ("NO")
