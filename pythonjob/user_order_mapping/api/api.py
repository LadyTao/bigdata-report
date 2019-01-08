#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('..')
sys.path.append('.')
from flask import current_app as app
from pprintpp import pprint as pp
from flask import request, jsonify,make_response, stream_with_context, Response
import os
import settings
import json
from elasticsearch import Elasticsearch
from flask import Blueprint, render_template

vp_api = Blueprint('vp_api', __name__)
es_host = settings.es_host
 
def gen_request(conditions):
    q = {
	    "aggs": {
		    "distinct_uid" : {
			    "cardinality" : {
				    "field" : "uid"
			    }
		    }
	    },
	    "query" : {
		    "bool" : {
		    }
	    },
            "sort": [{"_id": {"order":"desc"}}, "uid"]
    }
    start = (page_size)* (conditions['page']-1) if 'page' in conditions else 0
    start = 0 if start < 0 else start
    page_size = conditions.get("size", 20)
    order_only = True if 'order_only' in conditions else False
        
    should, must = [], []
    for field, terms in conditions.items():
        if field in ['size', 'page', 'order_only']:
            continue
        if field in ['inputtime', 'register_time']:
            if 'start' in terms:
                must.append({"range": {field: {"gte": terms['start'], "time_zone": "+08:00"}}})
            if 'end' in terms:
                must.append({"range": {field: {"lte": terms['end'], "time_zone": "+08:00"}}})
            continue

        if len(terms) == 0:
            continue
        if len(terms) >= 1:
            if type(terms) == str:
                must.append({"term": {field: terms}})
                continue
            if type(terms) == list:
                if len(terms) == 1:
                    must.append({"term": {field: terms[0]}})
                else:
                    for term in terms: #LIST
                        should.append({"term": {field: term}})
    if must:
        q["query"]["bool"]["must"] = must
    if should:
        q["query"]["bool"]["should"] = should
    return q, start, order_only, page_size

def make_query(start, page_size, q): 
    client = Elasticsearch(es_host)
    response = client.search(index='new_vp', from_=start, size=page_size, search_type='dfs_query_then_fetch', sort="_id:desc", preference='_only_local',  body=q)
    return response

def make_query_user(start, page_size,  uid_list):
    q = { "query" : { "bool" : {"should":[] }}}
    for uid in uid_list:
        q["query"]["bool"]["should"].append({"term": {'uid': uid}})

    client = Elasticsearch(es_host)
    print('start', start, json.dumps(q, ensure_ascii=False))
    response = client.search(index='customers', size=page_size, sort="_id:desc", preference='_only_local', body=q)
    return response


@vp_api.route("/user_csv", methods=['GET', 'POST'])
def user():
    q = request.args.get("q")
    conditions = json.loads(q)
    q, start, order_only, page_size = gen_request(conditions)

    def query_user(q, start, page_size):
        head = True
        row_count, uid_counts, missing_line = 0, 0, 0
        missing_uid_list = []
        while True:
            response = make_query(start, page_size,  q)
            uid_list = []
            fetch_size = response['aggregations']['distinct_uid']['value']
            #response['hits']['total']
            print("fetch_size", fetch_size, "uid_counts", uid_counts, "row_count", row_count, "start", start)
            for order in response['hits']['hits']:
                uid_list.append(order['_source']['uid'])
            uid_counts += len(uid_list)
             
            response = make_query_user(start, page_size, uid_list)
            if len(uid_list) == len(response['hits']['hits']):
                pass
            else:
                user_tag_id_list = [hit['_source']['uid'] for hit in response['hits']['hits']]
                for uid in uid_list:
                    if uid not in user_tag_id_list:
                        print('missing', uid)
                        missing_uid_list.append(uid)
                missing_line += (len(uid_list) - len(user_tag_id_list))
            
            for row in response['hits']['hits']:
                user = row['_source']
                if head == True:
                    #print(",".join(user.keys()))
                    yield(",".join(user.keys())+ "\n")
                    head = False
                row_data = []
                for k, v in user.items():
                    if type(v) is not str:
                        v = str(v)
                        v = v.replace(',',' ')
                    row_data.append(v)
            #print("<,>".join(row_data))
                row_count += 1
                yield(",".join(row_data) + "\n")
            start += page_size
            if fetch_size < start + page_size:
            #if row_count >= fetch_size:
                break
        #for uid in missing_uid_list:
        for i in range(missing_line+1):
            missing_list = []
            for j in range(18):
                missing_padding = "Missing"
                if j == 4:
                    if i < len(missing_uid_list):
                        missing_padding = str(missing_uid_list[i])
                missing_list.append(missing_padding)
            missing_str = ",".join(missing_list) 
            yield(missing_str + "\n")
        print(len(missing_uid_list),  missing_line, "fuck")
    return Response(query_user(q, start, page_size),  mimetype='text/csv')   
 
   
@vp_api.route("/order2user", methods=['GET', 'POST'])
def order2user():
    q = request.args.get("q")
    conditions = json.loads(q)
    q, start, order_only, page_size = gen_request(conditions)
    es_query = make_query(start, page_size, q)
    if 'hits' not in response:
        return "error"
    #total = response['hits']['total']
    total_hit = es_query['aggregations']['distinct_uid']['value']
    if order_only:
        pp(es_query['hits']['hits'])
        return jsonify(es_query['hits']['hits'])

    uid_list = [order['_source']['uid'] for order in es_query['hits']['hits']]
    response = make_query_user(start, page_size, uid_list)
    
    if page_size >= len(response['hits']['hits']):
        total = len(response['hits']['hits'])
    result = {
       'hit': response['hits']['hits'],
       'total': total_hit
    }
    return jsonify(result)

