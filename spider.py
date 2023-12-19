#!/usr/bin/env python3

import re
import time
import json
import requests
import datetime
import happybase

from tqdm import tqdm
from bs4 import BeautifulSoup

from urllib.parse import urljoin

# HBase连接和表设置
# 使用内存池，每次操作时新建连接，避免连接关闭
pool = happybase.ConnectionPool(size = 3, host = 'localhost', port = 9090, protocol = 'compact', transport = 'framed', autoconnect = False)

# 确保表格存在
with pool.connection() as conn :
  table_name = 'file_links'
  families = {'file_info': dict()}
  # 将其转换为 string
  table_names = [x.decode('utf8') for x in conn.tables()]
  if table_name not in table_names:
    conn.create_table(table_name, families)
  table = conn.table(table_name)
  conn.close()

def load_visited_url() :
  try :
    with open('visited_url.json', 'r', encoding = 'utf8') as f :
      visited_url = set(json.load(f))
  except FileNotFoundError :
    visited_url = set()
  return visited_url

def save_visited_url(visited_url) :
  with open('visited_url.json', 'w', encoding = 'utf8') as f :
    json.dump(list(visited_url), f, ensure_ascii = False, indent = 2)

visited_url = load_visited_url()

# 爬虫函数
def search(root_url, dep_limit: int) :
  '''
  爬取指定 url 中的文件，并访问期中的 url。 
  - dep_limit: 深度限制，每访问一个 url 减少 1，当为 0 的时候停止搜索。
  '''

  if dep_limit <= 0 :
    return []
  if root_url in visited_url :
    return []
  visited_url.add(root_url)

  # 添加延时，QPS约为10
  time.sleep(0.1)  # 每个请求之间暂停0.1秒
  file_data_list = []
  
  try :
    response = requests.get(root_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    for link in soup.find_all('a', href = True) :
      if link['href'].startswith('#') or link['href'].startswith('javascript') :
        continue
      real_url = urljoin(root_url, link['href'])
      if re.search('\.(pdf|txt|docx)$', real_url, re.IGNORECASE):
        file_type = real_url.split('.')[-1]
        file_data_list.append({
          'url': real_url,
          'type': file_type,
          'source': url,
          'date': datetime.datetime.now().strftime("%Y-%m-%d")
        })
      else :
        file_data_list.extend(search(real_url, dep_limit - 1))
  except Exception as e :
    print(f'Error while processing {root_url}')
    print(e)
  return file_data_list

def has_chinese(word) :
  pattern = re.compile(r'[\\u4e00-\\u9fa5]')
  if pattern.search(word) :
    return True
  return False


# 存储到 HBase 的函数
def store_data_in_hbase(data) :
  if len(data) == 0 :
    return
  with pool.connection() as conn :
    table = conn.table(table_name)
    for file_data in tqdm(data) :
      row_key = f"{file_data['source']}_{file_data['type']}_{file_data['url']}"
      try :
        table.put(row_key, {
          'file_info:url': file_data['url'],
          'file_info:type': file_data['type'],
          'file_info:source': file_data['source'],
          'file_info:date': file_data['date']
        })
      except Exception as e :
        print(f'Error while processing {row_key}')
    conn.close()

# 网站列表
urls = [
  #  'http://sds.ustc.edu.cn/main.htm',
    'http://sds.ustc.edu.cn/15443/list.htm',
    'https://zsb.ustc.edu.cn/main.htm',
    'http://www.job.ustc.edu.cn/index.htm',
  #  'https://www.teach.ustc.edu.cn/',
  #  'https://finance.ustc.edu.cn/main.htm',
    'https://finance.ustc.edu.cn/xzzx/list.psp',

    # 需要登录
    'https://xgyth.ustc.edu.cn/usp/home/main.aspx',

  #  'http://gradschool.ustc.edu.cn/',
    
    # 找到部分文件需要多级目录
    'http://gradschool.ustc.edu.cn/column/11'

    'https://bwc.ustc.edu.cn/5655/list.htm',
    'http://press.ustc.edu.cn/xzzq/main.htm',
    'http://ispc.ustc.edu.cn/6299/list.htm',
    'http://zhb.ustc.edu.cn/18534/list1.htm',

    # 有很多页面
    'http://young.ustc.edu.cn/15056/list.htm',
    
    # 'http://ustcnet.ustc.edu.cn/main.htm',
    'http://ustcnet.ustc.edu.cn/33419/list.htm',
    # 'https://zhc.ustc.edu.cn/main.htm',
    'https://zhc.ustc.edu.cn/wdxzn/list.htm',
    'http://cs.ustc.edu.cn/main.htm',
    'http://cybersec.ustc.edu.cn/main.htm',
    'https://math.ustc.edu.cn/main.htm',
    # 'https://sist.ustc.edu.cn/main.htm',
    'https://sist.ustc.edu.cn/5079/list.htm',
    # 'https://sz.ustc.edu.cn/index.html',
    'https://sz.ustc.edu.cn/wdxz_list/98-1.html',
    # 'https://sse.ustc.edu.cn/main.htm',
    'https://sse.ustc.edu.cn/wdxz_19877/list.htm',
    # 'https://iat.ustc.edu.cn/iat/index.html',
    'https://iat.ustc.edu.cn/iat/x161/'
    # 添加其余网址...
]

for i in range(1, 16) :
  urls.append(f'https://www.teach.ustc.edu.cn/download/all/page/{i}')

# 针对每个网站运行爬虫
bar = tqdm(urls)
bar.set_description('crawling web pages')
for url in bar:
  file_data_list = search(url, 2)
  store_data_in_hbase(file_data_list)
  save_visited_url(visited_url)

print('Finished!')