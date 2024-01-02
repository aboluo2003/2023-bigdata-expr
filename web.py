#!/usr/bin/env python3

from elasticsearch import Elasticsearch
from flask import Flask, send_from_directory, request

from config import WebConfig

es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

index_name = 'file_links'

# l = []
# for doc in es.scan(index = index_name) :
#   l.append(doc)

# print(len(l))

app = Flask(__name__)

@app.route('/')
def hello_world() :
  return send_from_directory(app.static_folder, 'index.htm')

@app.route('/query', methods = ['GET'])
def query_in_es() :
  args = request.args
  from_ = args.get('from', 0, int)
  size = args.get('size', 10, int)
  query = {
    'query': {
      'multi_match': {
        'query': args['q'],
        'fields': WebConfig.fields
      }
    },
    'track_total_hits': True,
    'from': from_,
    'size': size
  }

  search_result = es.search(index = index_name, body = query)
  return dict(search_result)

app.static_folder = 'static'
app.run(host = "0.0.0.0", port = 8080, debug = True)