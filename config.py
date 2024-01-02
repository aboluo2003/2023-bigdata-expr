class WebConfig :
  fields = ['title', 'type', 'path', 'root', 'url']

  mapping = {
    'properties': {
      'title': {'type': 'text'},
      'type': {'type': 'keyword'},
      'path': {'type': 'text'},
      'root': {'type': 'text'},
      'source': {'type': 'text'},
      'url': {'type': 'text'},
      'last-modified': {'type': 'text'},
      'crawl-time': {'type': 'date'}
    }
  }