#!/usr/bin/env python3

import time
import requests
from bs4 import BeautifulSoup
import re
import happybase
import datetime

# HBase连接和表设置
connection = happybase.Connection('localhost', 9000, autoconnect=False)
connection.open()
table_name = 'file_links'
families = {'file_info': dict()}
if table_name not in connection.tables():
  connection.create_table(table_name, families)
table = connection.table(table_name)

# 爬虫函数
def scrape_website(url):
  response = requests.get(url)
  soup = BeautifulSoup(response.content, 'html.parser')
  
  file_data_list = []
  for link in soup.find_all('a', href=True):
    if re.search('\.(pdf|txt|docx)$', link['href'], re.IGNORECASE):
      file_type = link['href'].split('.')[-1]
      file_data_list.append({
        'url': link['href'],
        'type': file_type,
        'source': url,
        'date': datetime.datetime.now().strftime("%Y-%m-%d")
      })
      
  return file_data_list

# 存储到HBase的函数
def store_data_in_hbase(data):
  for file_data in data:
    row_key = f"{file_data['source']}_{file_data['type']}_{file_data['url']}"
    table.put(row_key, {
      'file_info:url': file_data['url'],
      'file_info:type': file_data['type'],
      'file_info:source': file_data['source'],
      'file_info:date': file_data['date']
    })


# 网站列表
urls = [
    'http://sds.ustc.edu.cn/main.htm',
    'https://zsb.ustc.edu.cn/main.htm',
    'http://www.job.ustc.edu.cn/index.htm',
    'https://www.teach.ustc.edu.cn/',
    'https://finance.ustc.edu.cn/main.htm',
    'https://xgyth.ustc.edu.cn/usp/home/main.aspx',
    'http://gradschool.ustc.edu.cn/',
    'https://bwc.ustc.edu.cn/5655/list.htm',
    'http://press.ustc.edu.cn/xzzq/main.htm',
    'http://ispc.ustc.edu.cn/6299/list.htm',
    'http://zhb.ustc.edu.cn/18534/list1.htm',
    'http://young.ustc.edu.cn/15056/list.htm',
    'http://ustcnet.ustc.edu.cn/main.htm',
    'https://zhc.ustc.edu.cn/main.htm',
    'http://cs.ustc.edu.cn/main.htm',
    'http://cybersec.ustc.edu.cn/main.htm',
    'https://math.ustc.edu.cn/main.htm',
    'https://sist.ustc.edu.cn/main.htm',
    'https://sz.ustc.edu.cn/index.html',
    'https://sse.ustc.edu.cn/main.htm',
    'https://iat.ustc.edu.cn/iat/index.html',
    # 添加其余网址...
]

# 针对每个网站运行爬虫
for url in urls:
  file_data_list = scrape_website(url)
  store_data_in_hbase(file_data_list)

  # 添加延时，QPS约为10
  time.sleep(0.1)  # 每个请求之间暂停0.1秒