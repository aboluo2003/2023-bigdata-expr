from elasticsearch import Elasticsearch, helpers
import happybase

# 连接到Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# 连接到HBase
connection = happybase.Connection('localhost')
table = connection.table('file_links')

# 准备批量导入的数据
actions = []
for key, data in table.scan():
  doc = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
  action = {
    "_index": "file_links",
    "_id": key.decode('utf-8'),
    "_source": doc
  }
  actions.append(action)

# 执行批量导入
helpers.bulk(es, actions, request_timeout=1200, raise_on_error=False)