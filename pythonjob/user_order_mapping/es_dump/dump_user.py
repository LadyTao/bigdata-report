import user_format

from pyhive import hive
from TCLIService.ttypes import TOperationState
import time
from datetime import datetime, timezone
import elasticsearch
from elasticsearch import Elasticsearch
import json
from pprintpp import pprint as pp
from TCLIService.ttypes import TOperationState
import es_tool

es = Elasticsearch([{'host':'10.14.1.127','port':9200}])

def insert_one(index_name, type_name, order):
    try:
        #pp(order)
        doc = es.update(index=index_name,
                        doc_type=type_name,
                        id = order['id'],
                        body={'doc': order,
                              'doc_as_upsert':True})
        #print(doc)
        #if doc['_shards']['successful'] != 0:
        #    print(doc)
    except elasticsearch.ElasticsearchException as es1:
        print(sys.exc_info()[0])
        print(es1)
        print(order)
        time.sleep(1)


def convert_record_to_json(_, mapping):
    order = {}
    for field in user_format.order_fields:
        order[field] = _[mapping[field]]
    
        if field in user_format.field_convert_map:
            order[field] = user_format.field_convert_map[field](order[field])
        if field in user_format.convert_string_field:
            order[field] = order[field] if order[field] else 'Missing'
    return order


def get_field_mapping(cursor, table_name):
    """
    {
        'feild_name': field_idx,
        ....
        
    }
    """
    field_name_idx_map = {}

    sql = "describe %s"%table_name
    cursor.execute(sql)
    
    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()  
        status = cursor.poll().operationState
    record = cursor.fetchall()
    for idx, field_tuple in enumerate(record):
        name = field_tuple[0]
        field_name_idx_map[name] = idx
    return field_name_idx_map

if __name__ == "__main__":
    INDEX_NAME = "customers"
    TYPE_NAME = "customers"

    cursor = hive.connect(host='10.14.1.110', username='hive').cursor()
    cursor.execute("use mart")
    field_name_idx_map = get_field_mapping(cursor, 'user_tag')
    pp(field_name_idx_map)
    sql = "select * from user_tag "
    cursor.execute(sql , async=True)

    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        #for message in logs:
        #    print(message)     
        status = cursor.poll().operationState
    
    record = cursor.fetchone()
    idx = 1
    while record:
        if idx % 10000 == 0:
            print(idx)
        idx += 1  
        doc = convert_record_to_json(record, field_name_idx_map)
        insert_one(index_name=INDEX_NAME, type_name=TYPE_NAME, order=doc)
        #pp(doc)
        record = cursor.fetchone()
    print('done')

