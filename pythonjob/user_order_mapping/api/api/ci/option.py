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

ci_options = Blueprint('ci_options', __name__)
es_host = settings.es_host
with open('api/ci/search_template.json', "r") as template_file:
    template_source = template_file.read()    
 
options = {
    "channel": ["百度推广","胡萝卜周","易企传","搜狗推广","360推广","BD通用","BD-XL","BD-JJDS","baidu-mobile","BD-KSJKH","baidu-zhishi","baidu-pcinfo","麦本本","BD电商活动","赢商荟","智适应","bili","BZdownload","依凰蓝盾","常乐九九","栗子摄影器材","连锁正品店","BZdownload2","广点通推广","优设网","齐论电商","金山毒霸","腾讯管家","360管家","深圳高级中学","搜狗WAP注册","360WAP注册","神马WAP注册","10天试用-电商组自媒体","百度搜索推广WAP端","高校拓展","搜狗WAP下载","BD官网","BD-视达","神剪手BD微信社群新注册用户5天VIP","今日头条IOS","神剪手BD-QQ群李栋推广","百度搜索品牌推广PC端","微博红人推广（app）"],
    "productline": ["神剪手","神剪手移动端","其他"],
    "subcribe_type": ["月付","年付","其他"],
    "member_class": ["高级会员","VIP至尊会员","企业会员","其他"],
    "os_platform": ["Windows","Andriod","IOS","其他"],
    "payment_pattern": ["支付宝","微信","小程序","IOS支付","无需支付","其他"],
    "intv": ["1d", "1w", "1M"]
}
@ci_options.route("/ci_option", methods=['GET', 'POST'])
def ci_option():
    return jsonifyV(options)   

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

def apply_query_condition(template_source, q_obj):
    check_list = ['start', 'end', 'dim', 'intv', 'condition']

    start = q_obj.get("start", "2018-01-01")
    end = q_obj.get("end", "2018-01-10")
    dim = q_obj.get("dim", "productline")
    intv = q_obj.get("intv", "1d")
    condition = q_obj.get("condition", None)

    query = q_obj.get("query", {})

    query_template = template_source.replace("__START__", start)\
                                    .replace("__END__", end)\
                                    .replace("__INTV__", intv)\
                                    .replace("__DIM__", dim)
    print(query_template)
    query_obj = json.loads(query_template)
    pp(query_obj)  
    if condition is not None:
        must, should = parse_condition(condition)
        query_obj['query']['bool']['must'].extend(must)
        query_obj['query']['bool']['should'] = should
        if len(should) > 0:
            query_obj['query']['bool']['minimum_should_match'] = 1
    return query_obj

def make_query(es_query_obj):
    client = Elasticsearch(es_host)
    response = client.search(index='ci', search_type='dfs_query_then_fetch', sort="_id:desc", preference='_only_local',  body=es_query_obj)
    return response

@ci_options.route("/ci_online_stats", methods=['GET', 'POST'])
def ci_online_stats():
    q = request.args.get("q")
    q_obj = json.loads(q)
    es_query_obj = apply_query_condition(template_source, q_obj)
    pp(es_query_obj)
    response = make_query(es_query_obj)
    return jsonify(response)   
