#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
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
from flasgger import swag_from
es_ci_api = Blueprint('es_ci_api', __name__)
es_host = settings.es_host
with open('api/ci/search_template.json', "r") as template_file:
    template_source = template_file.read()    
with open('api/ci/compare_template.json', "r") as compare_template_file:
    compare_template_source = compare_template_file.read()    
 
options = {
    "channel": ["百度推广","胡萝卜周","易企传","搜狗推广","360推广","BD通用","BD-XL","BD-JJDS","baidu-mobile","BD-KSJKH","baidu-zhishi","baidu-pcinfo","麦本本","BD电商活动","赢商荟","智适应","bili","BZdownload","依凰蓝盾","常乐九九","栗子摄影器材","连锁正品店","BZdownload2","广点通推广","优设网","齐论电商","金山毒霸","腾讯管家","360管家","深圳高级中学","搜狗WAP注册","360WAP注册","神马WAP注册","10天试用-电商组自媒体","百度搜索推广WAP端","高校拓展","搜狗WAP下载","BD官网","BD-视达","神剪手BD微信社群新注册用户5天VIP","今日头条IOS","神剪手BD-QQ群李栋推广","百度搜索品牌推广PC端","微博红人推广（app）"],
    "productline": ["神剪手","神剪手移动端","其他"],
    "subscribe_type": ["月付","年付","其他"],
    "member_class": ["高级会员","VIP至尊会员","企业会员","其他"],
    "os_platform": ["Windows","Andriod","IOS","其他"],
    "payment_pattern": ["支付宝","微信","小程序","IOS支付","无需支付","其他"],
    "intv": ["1d", "1w", "1M"]
}

def parse_condition(condition):
    must, should = [], []
    for field, terms in condition.items():
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
    return must, should

def parse_query_obj(q_obj):
    q_obj['start'] = q_obj.get("start", "2018-01-01")
    q_obj['end'] = q_obj.get("end", "2018-01-10")
    q_obj['dim'] = q_obj.get("dim", "productline")
    q_obj['intv'] = q_obj.get("intv", "1d")
    q_obj['condition'] = q_obj.get("condition", None)
    q_obj['channel'] = q_obj.get("channel", ",".join(options['channel']))
    print(q_obj['condition'])
    if q_obj['condition'] is not None:
        q_obj['condition'] = json.loads(q_obj['condition'])
    q_obj['page'] = int(q_obj.get("page", '1'))
    q_obj['size'] = int(q_obj.get("size", '20'))
    return q_obj

def make_es_query_obj(template_source, q_obj):
    #check_list = ['start', 'end', 'dim', 'intv', 'condition']
    start = q_obj.get("start", "2018-01-01")
    end = q_obj.get("end", "2018-01-10")
    dim = q_obj.get("dim", "productline")
    intv = q_obj.get("intv", "1d")
    condition = q_obj.get("condition", None)

    query_template = template_source.replace("__START__", q_obj['start'])\
                                    .replace("__END__", q_obj['end'])\
                                    .replace("__INTV__", q_obj['intv'])\
                                    .replace("__DIM__", q_obj['dim'])

    query_obj = json.loads(query_template)
    if q_obj['dim'] == 'inputtime':
        query_obj['aggs']['intv']['aggs'] = query_obj['aggs']['intv']['aggs']['dim']['aggs']
    #pp(query_obj)  
    if condition is not None:
        must, should = parse_condition(condition)
        query_obj['query']['bool']['must'].extend(must)
        query_obj['query']['bool']['should'] = should
        if len(should) > 0:
            query_obj['query']['bool']['minimum_should_match'] = 1
    return query_obj

def make_es_query(es_query_obj):
    client = Elasticsearch(es_host)
    response = client.search(index='ci', search_type='dfs_query_then_fetch', sort="_id:desc", preference='_only_local',  body=es_query_obj)
    return response

def process_aggs(dim, aggs):
    '''
    input: complex es query result
    output:   
    [
       {
          time: time1,
          buckets: [
            {
               dim: dim_A 
               order_counts:
               user_counts:
               amount:
            }, 
            {...}
          ] 
      },{...}

    ]
    '''
    output = []
    for bucket in aggs['intv']['buckets']:
        data = {
            'time': bucket['key_as_string'],
            'buckets': []
        }
        if dim == 'inputtime':
            """
            dim_bucket_list = bucket['buckets']
            data['buckets'].append({
                  'dim': bucket['key_as_string'],
		  'order_counts': sum([dom_bucket['doc_count'] for dom_bucket in dim_bucket_list]),
                  'user_counts': sum([dom_bucket['UID']['value'] for dom_bucket in dim_bucket_list]),
                  'amount': sum([round(dom_bucket['amount']['value'],2) for dom_bucket in dim_bucket_list])}
            )
            """
            data['buckets'].append(
                 {
                   'dim': bucket['key_as_string'],
                   'order_counts': bucket['doc_count'],
                   'amount': round(bucket['amount']['value'], 2),
                   'user_counts': bucket['UID']['value']
                 }
            )
            output.append(data)
            continue
        for dim in bucket['dim']['buckets']:
            data['buckets'].append(
               { 
                  'dim' : dim['key'],
                  'order_counts': dim['doc_count'],
                  'user_counts': dim['UID']['value'],
                  'amount': round(dim['amount']['value'], 2)
               }
            )
        output.append(data)
    return output

def query_date_histograms(q_obj):
    es_query_obj = make_es_query_obj(template_source, q_obj)
    pp(es_query_obj)
    pp(json.dumps(es_query_obj))
    response = make_es_query(es_query_obj)
    aggs = response['aggregations']
    response = process_aggs(q_obj['dim'], aggs)
    return response

def get_dim_statistics_list(response):
    '''
    input:
    {
      { 
        time: ...,
        buckets: [
          {amount:..., dim: .., user_counts: .., dim:...,}
        ]
      }
    }    
    

    output:
    {
       [
         {'dim':.. , 'user_counts': .., 'order_counts': .., 'amount':...}
       ]  
    }
    '''
    '''
    dim_matrix_map = { 
       'dim': {'user_counts': .., 'order_counts': .., 'amount':...}
    }
    '''
    dim_matrix_map = {}
    for date_histograms in response:
        for bucket in date_histograms['buckets']:
            if bucket['dim'] not in dim_matrix_map:
                dim_matrix_map[bucket['dim']] = {'user_counts': 0, 'order_counts': 0, 'amount': 0}
            for key in bucket.keys():
                if key == 'dim': 
                    continue
                dim_matrix_map[bucket['dim']][key] += bucket[key]

    sorted_dim_matrix_map = collections.OrderedDict(sorted(dim_matrix_map.items()))

    dim_matric_list = []
    for dim, matrics_map in sorted_dim_matrix_map.items():
        matrics_map['dim'] = dim
        matrics_map['amount'] = round(matrics_map['amount'], 2) 
        dim_matric_list.append(matrics_map)
    
    return dim_matric_list

@es_ci_api.route("/ci_sales_option", methods=['GET'])
@swag_from('doc/ci_sales_option.yaml')
def ci_option():
    return jsonify(options)   

@es_ci_api.route("/es_ci_sales_graph", methods=['GET'])
@swag_from('doc/es_ci_sales_graph.yaml')
def ci_online_stats():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    
    response = query_date_histograms(q_obj)
    return jsonify(response)   

@es_ci_api.route("/es_ci_sales_table", methods=['GET'])
@swag_from('doc/es_ci_sales_table.yaml')
def es_ci_sales_table():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    date_histograms = query_date_histograms(q_obj)
    dim_statistics_list = get_dim_statistics_list(date_histograms)
    
    paged_list = []
    start_idx, end_idx = q_obj['size']*(q_obj['page']-1), q_obj['size']*q_obj['page']
    for idx, dim_statistics in enumerate(dim_statistics_list):
        if start_idx <= idx < end_idx:
            paged_list.append(dim_statistics)
    result = {
        'total': len(dim_statistics_list),
        'result': paged_list
    }
    return jsonify(result)

@es_ci_api.route("/es_ci_sales_compare", methods=['GET'])
@swag_from('doc/es_ci_sales_compare.yaml')
def es_ci_sales_compare():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    es_query_str = compare_template_source.replace("__START__", q_obj['start'])\
                                          .replace("__END__", q_obj['end'])
    es_query_obj = json.loads(es_query_str)
    response = make_es_query(es_query_obj)
    """response example:
    '_shards': {'failed': 0, 'skipped': 0, 'successful': 5, 'total': 5},
    'aggregations': {
        'amount': {'value': 358120.0384674072},
        'origin_amount': {'value': 733779.408416748},
        'uid': {'value': 7669},
    },
    'hits': {'hits': [], 'max_score': 0.0, 'total': 9504},
    'timed_out': False,
    'took': 11,
    """
    amount = response['aggregations']['amount']['value'] if 'aggregations' in response else 0
    origin_amount = response['aggregations']['origin_amount']['value'] if 'aggregations' in response else 0
    uid = response['aggregations']['uid']['value'] if 'aggregations' in response else 0
    result = {
        'title': q_obj['title'],
        '销售总额不含税': round(amount, 2),
        '销售净额': round(origin_amount, 2),
        '用户数': uid
    }
    return jsonify(result)


@es_ci_api.route("/es_ci_sales_csv", methods=['GET'])
@swag_from('doc/es_ci_sales_csv.yaml')
def es_ci_sales_csv():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    date_histograms = query_date_histograms(q_obj)
    dim_statistics_list = get_dim_statistics_list(date_histograms)
    def dict_to_str(dim, dim_statistics_list):
        import codecs
        yield codecs.BOM_UTF8
        yield "%s,user_counts,order_counts,amount\n"%dim
        for dim_statistics in dim_statistics_list:
            yield "%s,%s,%s,%s\n"%(dim_statistics['dim'],
                                 dim_statistics['user_counts'],
                                 dim_statistics['order_counts'],
                                 dim_statistics['amount'])
                                
    return Response(dict_to_str(q_obj['dim'], dim_statistics_list),  mimetype='text/csv')
