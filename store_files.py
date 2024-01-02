
import json
import hashlib
import happybase

from tqdm import tqdm

table_name = 'file_links'

# 连接到HBase
with open('file-links.json', 'r') as f :
  file_data_list = json.load(f)

# 存储到 HBase 的函数
def store_data_in_hbase(data, chunk_size = 256) :
  if len(data) == 0 :
    return
  key_feilds = ['title', 'type', 'url']
  conn = happybase.Connection(host = 'localhost', port = 9090, protocol = 'compact', transport = 'framed')
  table = conn.table(table_name)
  with table.batch() as batch :
    n = len(data)
    for l in tqdm(range(0, n, chunk_size)) :
      r = min(l + chunk_size, n)
      data_slice = data[l: r]
      for file_data in data_slice :
        key_data = {key: file_data[key] for key in key_feilds}
        row_key = hashlib.sha256(str(key_data).encode('utf8')).hexdigest().encode('utf8')
        for key, val in file_data.items() :
            batch.put(row_key, {f'file_info:{key}'.encode('utf8'): str(val).encode('utf8')})
      batch.send()
  conn.close()

store_data_in_hbase(file_data_list)