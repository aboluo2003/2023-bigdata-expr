
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm

import happybase

# 连接到Elasticsearch
# es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}], basic_auth=('elastic', 'NsXdtQtqB-tQEf9IkyHZ'))
es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])


# 连接到HBase
connection = happybase.Connection(host = 'localhost', port = 9090, protocol = 'compact', transport = 'framed', autoconnect = False)
table = connection.table('file_links')

mapping = {
  'properties': {
    'url': {'type': 'text'},
    'type': {'type': 'text'},
    'source': {'type': 'text'},
    'date': {'type': 'date'}
  }
}

index_name = 'file_links'
if not es.indices.exists(index = index_name) :
  es.indices.create(index = index_name)
  es.indices.put_mapping(index = index_name, body = mapping)
  print(f'build index {index_name} successfully')
else :
  print(f'index {index_name} already exists')

connection.open()
for b_key, b_data in tqdm(table.scan()) :
  key = b_key.decode('utf8')
  data = {x.decode('utf8').split(':')[-1]: y.decode('utf8') for x, y in b_data.items()}
  # es.index(index = index_name, )
  es.index(index = index_name, id = key, document = data)

connection.close()