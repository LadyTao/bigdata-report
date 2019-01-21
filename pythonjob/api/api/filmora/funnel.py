#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import current_app as app
from pprintpp import pprint as pp
from flask import request, jsonify,make_response, stream_with_context, Response
import os
import sys
sys.path.append('..')
sys.path.append('.')
import settings
#from utils.logger import get_logger
#import guess_order
import json
#import requests
#from flask import request

from elasticsearch import Elasticsearch

from flask import Blueprint, render_template

funnel_api = Blueprint('funnel_api', __name__)

def gen_request(conditions):
    q = {
	    "aggs": {
		    "distinct_uid" : {
			    "cardinality" : {
				    "field" : "uid"
			    }
		    }
	    }
	    ,
	    "query" : {
		    "bool" : {
		    }
	    },
            "sort": [{"_id": {"order":"desc"}}, "uid"]
    }
    order_only = False
    page_size, start = 20, 0
    if 'size' in conditions:
        page_size = conditions['size']
    if 'page' in conditions:
        start = (page_size)* (conditions['page']-1)
        if start < 0:
            start = 0
    if 'order_only' in conditions:
        order_only = True
        
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

def make_query(q, page_size=10000): 
    client = Elasticsearch('10.14.1.127')
    response = client.search(index='funnel', size=page_size, search_type='dfs_query_then_fetch', sort="_id:desc", preference='_only_local',  body=q)
    return response

def make_query_user(start, page_size,  uid_list):
    print("~~~~~~~~~~~~~~~~omg-------------------------")
    pp(uid_list)
    q = { "query" : { "bool" : {"should":[] }}}
    for uid in uid_list:
        q["query"]["bool"]["should"].append({"term": {'uid': uid}})
    client = Elasticsearch('10.14.1.127')
    print('start', start, json.dumps(q, ensure_ascii=False))
    response = client.search(index='customers', size=page_size, sort="_id:desc", preference='_only_local', body=q)
    return response

def gen_mock_result():
    import random
    result = []
    fields = ["visit", "download", "download_success", "install_success", 
              "regitst", "filmora_purchase", "resource_purchase"]
    orig_traffic = random.randint(100000, 1000000)
    total_cvr = 1
    for idx, field in enumerate(fields):
        cvr = round(random.uniform(0, 1), 2)
        print('cvr', cvr)
        total_cvr = total_cvr * cvr
        print("total_cvr", total_cvr)
        counts = int(orig_traffic * total_cvr)
        result.append({'stage': field, 'counts': counts, 'cvr': cvr, 'total_cvr':total_cvr})
    
    return result

def generate_funnel_result():
    return []

@funnel_api.route("/statistics", methods=['GET', 'POST'])
def statistics():
    q = request.args.get("q")
    q = json.loads(q)
    # day
    # task_id
    es_q = { "query" : { "bool" : {"must":[]}}}
    if 'day' in q:
        es_q["query"]["bool"]["must"].append({"range": {'day': {"gte": q['day'], "time_zone": "+08:00"}}})
        es_q["query"]["bool"]["must"].append({"range": {'day': {"lte": q['day'], "time_zone": "+08:00"}}})
    if 'range' in q and 'start' in q['range']:
       es_q["query"]["bool"]["must"].append({"range": {'day': {"gte": q['range']['start'], "time_zone": "+08:00"}}})
    if 'range' in q and 'end' in q['range']:
       es_q["query"]["bool"]["must"].append({"range": {'day': {"lte": q['range']['end'], "time_zone": "+08:00"}}})
    es_q["query"]["bool"]["must"].append({"term": {'task_id': q['task_id']}})

    #must.append({"range": {field: {"gte": terms['start'], "time_zone": "+08:00"}}})
    pp(es_q)
    client = Elasticsearch('10.14.1.127')
    response = client.search(index='statistics', search_type='dfs_query_then_fetch', sort="_id:desc", preference='_only_local',  body=es_q)
    result = []
    for hits in response['hits']['hits']:
        result.append(hits['_source'])

 
    if 'major' in q and q['task_id'] == 'dev_version_counts':
        major_result = {
        }
        for version_count_map in result[0]['value']:
            major_version = version_count_map['version'][0:2*q['major']-1]
            if major_version not in major_result:
                major_result[major_version] = 0
            major_result[major_version] += version_count_map['dev_count']
            '''
            for major_version, major_counts in major_result.items():
                if version_count_map['version'][0] == major_version:
                    major_result[major_version] += version_count_map['dev_count']
            '''
        result[0]['value'] = []
        for major_version, major_counts in major_result.items():
            result[0]['value'].append({'version': major_version[0:-1] if major_version[-1]=='.'  else  major_version,
                                       'dev_count': major_counts})
    return jsonify(result)
    

@funnel_api.route("/funnel2", methods=['GET', 'POST'])
def funnel2():
    cube_mode = False
    cube_only = ['platform', 'productversion', 'name']
    skip_options = ['start', 'end']
    q = request.args.get("q")
    q = json.loads(q)
    for k, v in q.items():
        if k in cube_only:
            cube_mode = True
    es_q = { "query" : { "bool" : {"must":[]}}}
    must, should = [], []
    for k, v in q.items():
        if k in skip_options:
            continue
        if len(v) >= 1:
            if type(v) == str:
                must.append({"term": {k: v}}) 
            if type(v) == list:
                if len(v) == 1:
                    must.append({"term": {k: v[0]}})
                else: 
                    for term in v:
                        should.append({"term": {k: term}})
    if must:
        es_q["query"]["bool"]["must"] = must
    if should:
        es_q["query"]["bool"]["should"] = should
    #if cube_mode:
    pp(es_q)     
    response = make_query(es_q)
    pp(response)
    matrix = {
        'resource_purchase':0,
        'filmora_purchase':0, 
        'regitst':0,
        'install_success':0,
        'download_success':0,
        'download':0,
        'visit': 0
    }
    site_hit = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        if source['site'] not in site_hit:
            site_hit.append(source)
        for k, v in matrix.items():
            if k == 'download_success' and k not in source:
                matrix['download_success'] = source['download_sucess']
                continue
            matrix[k] += source[k]
    if cube_mode:
        matrix['visit'] = 0
        for site in site_hit:
            should.append({"term": {'site': site}})   
        es_q["query"]["bool"]["should"] = should
        es_q["query"]["bool"]["must"] = [
           {"term": {'start': q['start']}},
           {"term": {'end': q['end']}},
           {"term": {'period': q['period']}}
        ] 
        response = make_query(es_q)
        for hit in response['hits']['hits']:
            source = hit['_source']
            matrix['visit'] += source['visit']
    if matrix['download_success'] == 0:
        matrix['download_success'] = int(matrix['download'] * 0.8)
    if matrix['regitst'] == 0:
        matrix['regitst'] = int(matrix['install_success'] * 0.6)
  
    result = []
    stage_list = ['visit', 'download', 'download_success', 'install_success', 'regitst', 'filmora_purchase', 'resource_purchase']
    for stage in stage_list:
        stage_stat = { 
            'counts': matrix[stage],
            'stage': stage
        }
        if stage == 'visit':
            nv = matrix['visit']
            last_count = matrix['visit']
            stage_stat['total_cvr'] = stage_stat['cvr'] = 1
        else:    
            stage_stat['cvr'] = matrix[stage]/last_count       
            stage_stat['total_cvr'] = matrix[stage]/nv
            last_count = matrix[stage]
        result.append(stage_stat)
    pp(matrix)
    pp(site_hit)
    print(site_hit)
    return jsonify({'matrix':matrix, 'result':result})

@funnel_api.route("/funnel", methods=['GET', 'POST'])
def funnel():
    result = gen_mock_result()
    
    return jsonify(result)

