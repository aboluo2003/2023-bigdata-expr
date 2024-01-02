#!/usr/bin/env python3

import re
import time
import json
import queue
import hashlib
import requests
import datetime
import warnings
import traceback
import happybase

from tqdm import tqdm
from bs4 import BeautifulSoup

from datetime import datetime

from urllib.parse import urljoin, urlparse

# 确保表格存在
conn = happybase.Connection(host = 'localhost', port = 9090, protocol = 'compact', transport = 'framed')
table_name = 'file_links'
families = {'file_info': dict()}
# 将其转换为 string
table_names = [x.decode('utf8') for x in conn.tables()]
if table_name not in table_names:
  conn.create_table(table_name, families)
table = conn.table(table_name)
conn.close()

def load_visited_url() :
  warnings.warn('visited_url.json is not loaded')
  return set({})
  try :
    with open('visited_url.json', 'r', encoding = 'utf8') as f :
      visited_url = set(json.load(f))
  except FileNotFoundError :
    visited_url = set()
  return visited_url

def save_json(obj, filename) :
  with open(filename, 'w', encoding = 'utf8') as f :
    json.dump(obj, f, ensure_ascii = False, indent = 2)

def save_visited_url(visited_url) :
  with open('visited_url.json', 'w', encoding = 'utf8') as f :
    json.dump(list(visited_url), f, ensure_ascii = False, indent = 2)
def save_log() :
  with open('log.json', 'w', encoding = 'utf8') as f :
    json.dump(log_json, f, ensure_ascii = False, indent = 2)

log_json = {
  'errors': [],
  'prevented': [],
  'not-html': []
}
visited_url = load_visited_url()

def get_visit_path(visit_stack) :
  return '->'.join([x['desc'] for x in visit_stack])

def is_visitable(link) :
  href = link['href']
  return (not href.startswith('#')) and (not href.startswith('javascript')) and (not href.startswith('mailto'))

def is_ustc_domain(url) :
  parsed_url = urlparse(url)
  if not parsed_url.netloc.endswith('.ustc.edu.cn') :
    return False
  return True

def is_downloadable(url) :
  return re.search('\.(pdf|txt|docx|doc|xlsx|xls|ppt|pptx|tex|zip|csv|rar|tar\.gz|mp4)$', url, re.IGNORECASE) 

# 爬虫函数
def search(url_list, dep_limit: int, prevent_not_ustc: bool = True, auto_save_interval = 100, timeout = 2) :
  
  '''
  爬取指定 url 中的文件，并访问期中的 url。 
  - dep_limit: 深度限制，每访问一个 url 减少 1，当为 0 的时候停止搜索。
  '''

  Q = queue.Queue()

  for root_url, desc in url_list :
    Q.put({
      'url': root_url,
      'stack': [{
        'url': root_url,
        'desc': desc
      }],
      'ttl': dep_limit
    })
    visited_url.add(root_url)

  def save_queue_cache() :
    Qjson = list(Q.queue)
    with open('queue.json', 'w', encoding='utf8') as f :
      json.dump(Qjson, f, ensure_ascii = False, indent = 2)

  file_data_list = []
  err_count = 0
  bar = tqdm()

  while not Q.empty() :
    element = Q.get()
    url = element['url']
    ttl = element['ttl']
    if err_count == 0 :
      bar.set_description(f'Processing {url}')
    else :
      bar.set_description(f'{err_count} error(s) occurred. Procession {url}')
    bar.update(1)
    if bar.n % auto_save_interval == 0 :
      save_visited_url(visited_url)
      save_queue_cache()
      save_json(file_data_list, 'file-links.json')
      save_log()
    try :
      content_type = requests.head(url, timeout = timeout).headers.get('Content-Type', '')

      # 检查 Header 中是否有 html
      if (not 'html' in content_type.lower()) and (not content_type == '') and (not content_type == 'text/plain'):
        log_json['not-html'].append({
          'url': url,
          'content-type': content_type
        })
        continue
      response = requests.get(url, timeout = timeout)
      soup = BeautifulSoup(response.content, 'html.parser')
      
      time.sleep(0.1)  # 每个请求之间暂停0.1秒

      for link in soup.find_all('a', href = True) :
        if not is_visitable(link) :
          continue

        real_url = urljoin(url, link['href'])
        if prevent_not_ustc and not is_ustc_domain(real_url) :
          parsed_url = urlparse(real_url)
          log_json['prevented'].append({
            'url': real_url,
            'netloc': parsed_url.netloc
          })
          continue

        descript = link.get_text()

        if real_url in visited_url :
          continue
        visited_url.add(real_url)

        last_modified = response.headers.get('Last-Modified', '')
        if len(last_modified) > 0 :
          date = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
          last_modified = date.strftime('%Y-%m-%d')

        if is_downloadable(real_url) :
          file_type = real_url.split('.')[-1]

          file_data_list.append({
            'title': descript,
            'type': file_type,
            'path': get_visit_path(element['stack']),
            'root': element['stack'][0]['desc'],
            'source': url,
            'url': real_url,
            'last-modified': last_modified,
            'crawl-time': datetime.now().strftime("%Y-%m-%d")
          })
        elif ttl > 0 :
          next_stack = list(element['stack'])
          next_stack.append({
            'url': real_url,
            'desc': descript
          })
          Q.put({
            'url': real_url,
            'stack': next_stack,
            'ttl': ttl - 1
          })
    except KeyboardInterrupt as e :
      save_log()
      save_queue_cache()
      save_visited_url(visited_url)
      break
    except Exception as e :
      log_json['errors'].append({
        'location': url,
        'msg': traceback.format_exc() 
      })
      err_count += 1
      save_log()
  return file_data_list

# 存储到 HBase 的函数
def store_data_in_hbase(data) :
  if len(data) == 0 :
    return
  key_feilds = ['title', 'type', 'url']
  conn = happybase.Connection(host = 'localhost', port = 9090, protocol = 'compact', transport = 'framed')
  table = conn.table(table_name)
  with table.batch() as batch :
    for file_data in tqdm(data) :
      key_data = {key: file_data[key] for key in key_feilds}
      row_key = hashlib.sha256(str(key_data).encode('utf8')).hexdigest().encode('utf8')
      for key, val in file_data.items() :
        batch.put(row_key, {f'file_info:{key}'.encode('utf8'): str(val).encode('utf8')})
    batch.send()
  conn.close()

raw_text = r"""中科大本科生招生网 https://zsb.ustc.edu.cn/main.htm
中科大就业信息网 http://www.job.ustc.edu.cn/index.htm
中科大教务处 https://www.teach.ustc.edu.cn/
中科大财务处 https://finance.ustc.edu.cn/main.htm
学工一体化 https://xgyth.ustc.edu.cn/usp/home/main.aspx
中科大研究生院 http://gradschool.ustc.edu.cn/
中科大保卫与校园管理处 https://bwc.ustc.edu.cn/5655/list.htm
中科大出版社 http://press.ustc.edu.cn/xzzq/main.htm
中科大信息科学实验中心 http://ispc.ustc.edu.cn/6299/list.htm
中科大科技成果转移转化办公室 http://zhb.ustc.edu.cn/18534/list1.htm
青春科大 http://young.ustc.edu.cn/15056/list.htm
中科大网络信息中心 http://ustcnet.ustc.edu.cn/main.htm
中科大资产与后勤保障处 https://zhc.ustc.edu.cn/main.htm
中科大计算机科学与技术学院 http://cs.ustc.edu.cn/main.htm
中科大网络空间安全学院 http://cybersec.ustc.edu.cn/main.htm
中科大数学科学学院 https://math.ustc.edu.cn/main.htm
中科大信息科学技术学院 https://sist.ustc.edu.cn/main.htm
中科大苏州高等研究院 https://sz.ustc.edu.cn/index.html
中科大软件学院 https://sse.ustc.edu.cn/main.htm
中科大先进技术研究院 https://iat.ustc.edu.cn/iat/index.html"""

# 网站列表
urls = [
  ('http://sds.ustc.edu.cn/', '中科大大数据学院'),
]

# warnings.warn('only one site for test')
for line in raw_text.split('\n') :
  desc, url = line.split()
  urls.append((url, desc))

file_data_list = search(urls, 3)

save_json(file_data_list, 'file-links.json')

store_data_in_hbase(file_data_list)
save_visited_url(visited_url)
save_log()

print('Finished!')
print(str(len(visited_url)) + ' pages were found!')
print(str(len(file_data_list)) + ' files were found!')