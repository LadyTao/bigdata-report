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
from flasgger import swag_from
import pymysql.cursors

import settings
host = settings.renew_mysql_host
port = settings.renew_mysql_port
user=  settings.renew_mysql_user
password = settings.renew_mysql_password
#db = 'data_sale'
db = settings.renew_mysql_db

renew_api = Blueprint('renew_api', __name__)
es_host = settings.es_host
 
options = {
        "channel": ["百度推广","胡萝卜周","易企传","搜狗推广","360推广","BD通用","BD-XL","BD-JJDS","baidu-mobile","BD-KSJKH","baidu-zhishi","baidu-pcinfo","麦本本","BD电商活动","赢商荟","智适应","bili","BZdownload","依凰蓝盾","常乐九九","栗子摄影器材","连锁正品店","BZdownload2","广点通推广","优设网","齐论电商","金山毒霸","腾讯管家","360管家","深圳高级中学","搜狗WAP注册","360WAP注册","神马WAP注册","10天试用-电商组自媒体","百度搜索推广WAP端","高校拓展","搜狗WAP下载","BD官网","BD-视达","神剪手BD微信社群新注册用户5天VIP","今日头条IOS","神剪手BD-QQ群李栋推广","百度搜索品牌推广PC端","微博红人推广（app）"],
    "expire_plan": ["月付", "年付"]
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
    q_obj['start'] = q_obj.get("start", "2018-12-01")
    q_obj['end'] = q_obj.get("end", "2019-01-31")
    q_obj['channel'] = q_obj.get("channel", ','.join(options['channel']))
    q_obj['channel'] = "','".join(q_obj['channel'].split(","))
    q_obj['subtype'] = q_obj.get("subtype", "month")
    q_obj['intv'] = q_obj.get("intv", "1d") #1M
    q_obj['same_type'] = False if q_obj.get("same_type", 'false')=="false" else True
    q_obj['page'] = int(q_obj.get("page", '1'))
    q_obj['size'] = int(q_obj.get("size", '200000'))
    return q_obj


@renew_api.route("/ci_retention_option", methods=['GET'])
@swag_from('doc/retention_option.yaml')
def ci_retention_option():
    return jsonify(options)


def gen_sql(q_obj):
    sql = """
	select 
	temp.stat_intv, 
	ifnull(ci.total_renew_user, 0) as total_renew_user, 
	ifnull(ci.total_renew_rate, 0) as total_renew_rate ,
	ifnull(ci.senior_renew_user, 0) as senior_renew_user, 
	ifnull(ci.senior_renew_rate, 0) as senior_renew_rate ,
	ifnull(ci.vip_renew_user, 0) as vip_renew_user ,
	ifnull(ci.vip_renew_rate, 0) as vip_renew_rate ,
	ifnull(ci.enter_renew_user, 0) as enter_renew_user, 
	ifnull(ci.enter_renew_rate, 0) as enter_renew_rate

	from (
	select __INTV_FIELD__ as stat_intv from
	    (
	       select adddate('1970-01-01',t4*10000 + t3*1000 + t2*100 + t1*10 + t0) stat_date from
		 (select 0 t0 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
		 (select 0 t1 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
		 (select 0 t2 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
		 (select 0 t3 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
		 (select 0 t4 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4
	       ) v
	     Where stat_date between '__START__' and '__END__'
	     group by stat_intv
	) as temp left join 
	(

            select __INTV_FIELD__  as stat_intv,
    	    sum(renew_user) as total_renew_user,
    	    sum(renew_user)/sum(expire_user) as total_renew_rate,
	    sum(case when expire_user_level="高级会员" then renew_user else 0 end) as senior_renew_user,
	    sum(case when expire_user_level="高级会员" then renew_user else 0 end)/sum(case when expire_user_level="高级会员" then expire_user else 0 end) as senior_renew_rate,
	    sum(case when expire_user_level="VIP会员" then renew_user else 0 end) as vip_renew_user,
	    sum(case when expire_user_level="VIP会员" then renew_user else 0 end)/sum(case when expire_user_level="VIP会员" then expire_user else 0 end) as vip_renew_rate,
	    sum(case when expire_user_level="企业会员" then renew_user else 0 end) as enter_renew_user,
	    sum(case when expire_user_level="企业会员" then renew_user else 0 end)/sum(case when expire_user_level="企业会员" then expire_user else 0 end) as enter_renew_rate
	    from ci_member_renew_rate_day
	    where channel in ('__CHANNEL_LIST___') AND expire_time_type='__EXPIRE_TYPE__' 
            AND stat_date>='__START__' AND stat_date<'__END__' 
            __AND__SAME_TYPE__ 
 
           group by stat_intv
           order by stat_intv)
       as ci 
       on temp.stat_intv = ci.stat_intv 
       LIMIT __PAGE_START__, __PAGE_SIZE__ 
    """
    intv_field = "stat_date" if q_obj['intv'] == '1d' else "DATE_FORMAT(stat_date,'%Y-%m')"
    and_same_type = 'AND expire_user_level=renew_user_level AND expire_time_type=renew_time_type ' if q_obj['same_type'] else ''
    sql = sql.replace("__INTV_FIELD__", intv_field)\
             .replace("__CHANNEL_LIST___", q_obj['channel'])\
             .replace("__EXPIRE_TYPE__", q_obj['expire_time_type'])\
             .replace("__AND__SAME_TYPE__", and_same_type)\
             .replace("__START__", q_obj['start'])\
             .replace("__END__", q_obj['end'])\
             .replace("__PAGE_START__", str((q_obj['page']-1)))\
             .replace("__PAGE_SIZE__", str(q_obj['size']))
     
    return sql   

def query_retention_data(q_obj):
    ci_mysql = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8mb4', cursorclass=pymysql.cursors.SSDictCursor)

    result = []
    with ci_mysql.cursor() as cursor:

        sql_query = gen_sql(q_obj)  
        print(sql_query)
        cursor.execute(sql_query)
        record_list = cursor.fetchall()
        
        for record in record_list:
            data = {
               "time": record['stat_intv'],
               "buckets":[]} 
            # stat_inv, total_renew_user, ....
            dim_col_map = {
              '企業': 'enter', 'VIP': 'vip', '高级': 'senior', '总': 'total'
            }
            for dim, col in dim_col_map.items():
                user_col, rate_col = '%s_renew_user'%col, '%s_renew_rate'%col
                bucket = {
                  'dim': dim+"会员", 
                  'renew_user': int(record[user_col]) if record[user_col] else 0,
                  'renew_rate': float(record[rate_col]) if record[rate_col] else 0
                }
                data['buckets'].append(bucket)
            result.append(data)
    ci_mysql.close()
    return result
 
def get_dim_statistics_list(graph_plot_list):
    result = []
    for graph_plot in graph_plot_list:
        data = {
            u'时间': graph_plot['time'],
        }
        for bucket in graph_plot['buckets']:
            data_key = bucket['dim'] + u"续费数"
            data[data_key] = bucket["renew_user"]
            data_key = bucket['dim'] + u"续费率"
            data[data_key] = bucket["renew_rate"]
        result.append(data)
    return result


@renew_api.route("/ci_retention_graph", methods=['GET'])
@swag_from('doc/retention_graph.yaml')
def ci_retention_graph():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    date_histograms = query_retention_data(q_obj)
    return jsonify(date_histograms)


@renew_api.route("/ci_retention_table", methods=['GET'])
@swag_from('doc/retention_table.yaml')
def ci_retention_table():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    date_histograms = query_retention_data(q_obj)
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


@renew_api.route("/ci_retention_csv", methods=['GET'])
@swag_from('doc/retention_csv.yaml')
def ci_retention_csv():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    date_histograms = query_retention_data(q_obj)
    dim_statistics_list = get_dim_statistics_list(date_histograms)

    key_list = ["时间","总会员续费数","总会员续费率","高级会员续费数","高级会员续费率","VIP会员续费数","VIP会员续费率"]
    def dict_to_str(dim_statistics_list):
        yield ",".join(key_list)+"\n"
        for dim_statistics in dim_statistics_list:
            dim_statistics_str = ",".join([str(dim_statistics[key]) for key in key_list]) + "\n"
            yield dim_statistics_str
    return Response(dict_to_str(dim_statistics_list),  mimetype='text/csv')


@renew_api.route("/ci_retention_pie_chart", methods=['GET'])
@swag_from('doc/retention_pie_chart.yaml')
def ci_retention_piechart():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
   
    ci_mysql = pymysql.connect(host=host, port=port, user=user, password=password, db=db, 
                               charset='utf8mb4', cursorclass=pymysql.cursors.SSDictCursor)

    result = []
    with ci_mysql.cursor() as cursor:
        sql  = """
            select __INTV_FIELD__ as stat_intv,
            renew_time_type, renew_user_level, sum(renew_user) as renew_user_counts
	    from ci_member_renew_rate_day
	    where channel in ('__CHANNEL_LIST___') AND expire_time_type='__EXPIRE_TYPE__' 
            AND stat_date>='__START__' AND stat_date<'__END__' 
            AND renew_user > 0 
            AND expire_user_level __USER_LEVEL__  
            __AND__SAME_TYPE__ group by stat_intv, renew_user_level, renew_time_type
        """

        intv_field = "stat_date" if q_obj['intv'] == '1d' else "DATE_FORMAT(stat_date,'%Y-%m')"
        if q_obj['expire_user_level'] == "总会员":
            expire_user_level =  "in ('高级会员','VIP会员','企业会员')"
        else:
            expire_user_level = "='%s'" %q_obj['expire_user_level']
        and_same_type = ' AND expire_user_level=renew_user_level AND expire_time_type=renew_time_type ' if q_obj['same_type'] else ''
        sql_query = sql.replace("__INTV_FIELD__", intv_field)\
                 .replace("__CHANNEL_LIST___", q_obj['channel'])\
                 .replace("__EXPIRE_TYPE__", q_obj['expire_time_type'])\
                 .replace("__AND__SAME_TYPE__", and_same_type)\
                 .replace("__START__", q_obj['start'])\
                 .replace("__END__", q_obj['end'])\
                 .replace("__USER_LEVEL__", expire_user_level)\
                 .replace("__PIE_TIME__", q_obj['pie_time'])
        print(sql_query)
        cursor.execute(sql_query)
        record_list = cursor.fetchall()
        """
            {
		'renew_time_type': '月付',
		'renew_user_counts': Decimal('2'),
		'renew_user_level': 'VIP会员',
		'stat_intv': datetime.date(2019, 1, 17),
	    },
	    {
		'renew_time_type': '月付',
		'renew_user_counts': Decimal('1'),
		'renew_user_level': '高级会员',
		'stat_intv': datetime.date(2019, 1, 17),
	    },
	    {
		'renew_time_type': '月付',
		'renew_user_counts': Decimal('1'),
		'renew_user_level': 'VIP会员',
		'stat_intv': datetime.date(2019, 1, 20),
	    },
        """
        if q_obj['center'] == 'renew_time_type':
            center_key, circle_key = 'renew_time_type', 'renew_user_level'
        else:
            center_key, circle_key = 'renew_user_level', 'renew_time_type'
        base_map = {}  
        total = 0
        for record in record_list:
            renew_user_counts = int(record['renew_user_counts'])
            total += renew_user_counts
            center_value, circle_value = record[center_key], record[circle_key]
            #print(record['renew_time_type'], record['renew_user_level'], record['renew_user_counts'], record['stat_intv'], total)
            if center_value not in base_map:
                base_map[center_value] = {'counts':0, 'title': center_value, 'time': record['stat_intv'], 'buckets':{}}
            if circle_value  not in base_map[center_value]['buckets']:
                base_map[center_value]['buckets'][circle_value] = {'counts':0, 'percent':0, 'title': center_value+"-"+circle_value}
            base_map[center_value]['counts'] += renew_user_counts
            base_map[center_value]['buckets'][circle_value]['counts'] += renew_user_counts
        for center_value, center_map in base_map.items():
            for circle_value, circle_map in center_map['buckets'].items():
                circle_map['percent'] = round(circle_map['counts'] / total, 3)
        base_map['total'] = total
        ci_mysql.close()
    return jsonify(base_map)
   
