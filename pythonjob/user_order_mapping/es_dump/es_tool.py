import sys
print(sys.stdout.encoding)
from pprintpp import pprint as pp
from elasticsearch import Elasticsearch
import time

es = Elasticsearch([{'host':'10.14.1.127','port':9200}])

def insert_one(body):
    try:
        doc = es.update(index='vp',
                        doc_type='orders',
                        id=body['id'],
                        body={'doc': body,
                              'doc_as_upsert':True})
        print(doc)
        if doc['_shards']['successful'] != 0:
            print(idx, doc)
    except:
        pp(doc)
        
        time.sleep(10)
