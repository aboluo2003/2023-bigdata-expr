
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm

from config import WebConfig
import happybase

# 连接到Elasticsearch
# es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}], basic_auth=('elastic', 'NsXdtQtqB-tQEf9IkyHZ'))
es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

connection = happybase.Connection(host = 'localhost', port = 9090, protocol = 'compact', transport = 'framed', autoconnect = False)
table_name = 'file_links'
table = connection.table(table_name)

mapping = WebConfig.mapping

print(mapping)

index_name = 'file_links'

# es.indices.delete(index = index_name)

if not es.indices.exists(index = index_name) :
  es.indices.create(index = index_name)
  # es.indices.put_mapping(index = index_name, body = mapping)
  print(f'build index {index_name} successfully')
else :
  print(f'index {index_name} already exists')

connection.open()
for b_key, b_data in tqdm(table.scan()) :
  key = b_key.decode('utf8')
  data = {x.decode('utf8').split(':')[-1]: y.decode('utf8') for x, y in b_data.items() if x.decode('utf8') != 'file_info:last-modified'}
  es.index(index = index_name, id = key, document = data)

connection.close()