#!/usr/bin/env python3

from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch, helpers


app = Flask(__name__)


@app.route('/')
def index():
	return 'Hello, World!'

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# 第三部分：设置 Elasticsearch 索引
def setup_elasticsearch():
    index_name = 'file_links'
    mapping = {
        'mappings': {
            'properties': {
                'url': {'type': 'keyword'},
                'type': {'type': 'text'},
                'source': {'type': 'keyword'},
                'date': {'type': 'date'}
            }
        }
    }

    # 创建索引
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
    print("Elasticsearch index setup complete.")

@app.route('/search', methods=['GET'])
def search():
	query = request.args.get('query', '')  # 获取查询参数
	page = request.args.get('page', 1, type=int)  # 分页参数
	size = request.args.get('size', 10, type=int)  # 每页大小
	
	# 构建搜索请求
	search_body = {
		'query': {
			'multi_match': {
				'query': query,
				'fields': ['url', 'type', 'source', 'date']  # 根据需要调整字段
			}
		},
		'from': (page - 1) * size,
		'size': size
	}
	
	# 执行搜索
	response = es.search(index='file_links', body=search_body)
	return jsonify(response['hits']['hits'])

if __name__ == '__main__':
    # 设置 Elasticsearch 索引
    setup_elasticsearch()

    # 运行 Flask 应用
    app.run(host='localhost', port=5000, debug=True)  # 运行在5000端口
