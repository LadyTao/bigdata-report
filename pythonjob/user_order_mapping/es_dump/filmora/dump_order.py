import order_format
import time
from datetime import datetime, timezone
import elasticsearch
from elasticsearch import Elasticsearch
import json
import es_tool
from pprintpp import pprint as pp
from pyhive import hive
from TCLIService.ttypes import TOperationState
from pyhive.exc import OperationalError
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
    for field in order_format.order_fields:
        order[field] = _[mapping[field]]
    for field in order_format.order_fields:
        if field in order_format.field_convert_map:
            order[field] = order_format.field_convert_map[field](order[field])
        if field in order_format.convert_float_fields:
            order[field] = float(order[field]) if order[field] else 0
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

    INDEX_NAME = "new_vp"
    TYPE_NAME = "orders"


    cursor = hive.connect(host='10.14.1.110', username='hive').cursor()
    cursor.execute("use mart")
    field_name_idx_map = get_field_mapping(cursor, 'order_clean')

    sql = "select * from order_clean "
    cursor.execute(sql , async=True)

    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        #for message in logs:
        #    print(message)     
        status = cursor.poll().operationState
    
    record = cursor.fetchone()
    idx = 1
    operational_error_count = 0
    id_none_count = 0
    while record:
        if idx % 10000 == 0:
            print(idx, id_none_count)
        idx += 1  
        doc = convert_record_to_json(record, field_name_idx_map)
        if doc['id'] is None:
            #print(doc)
            id_none_count += 1
            continue
        insert_one(index_name=INDEX_NAME, type_name=TYPE_NAME, order=doc)
        try: 
            record = cursor.fetchone()
        except OperationalError:
            operational_error_count += 1
        #pp(doc)
        #break
    print('idx:',idx)
    print('id_none_count:',id_none_count)
    print('operational_error_count:', operational_error_count)    
    print('done')
    print('id_none_count:',id_none_count)

