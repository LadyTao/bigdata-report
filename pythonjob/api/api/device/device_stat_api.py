#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import sys

sys.path.append('..')
sys.path.append('.')
from flask import current_app as app
from pprintpp import pprint as pp
from flask import request, jsonify, make_response, stream_with_context, Response
import os

import json

from flask import Blueprint, render_template
from flasgger import swag_from

device_api = Blueprint('device_api', __name__)

import pymysql.cursors
import settings

db = settings.device_db

options = {
    "app_version": ['9.0.7', '9.0.7.2', '9.1.0', '9.0.8.0', '9.0.8.2', '9.0.7.4',
                   '9.1.2.0', '9.1.1.2', '9.1.1.0', '9.1.0.11', '9.1.0.2',
                   '9.1.0.0', '9.1.0.5', '3.0.0.7', '9.1.0.10', '9.1.0.8',
                   '9.1.0.12', '9.1.0.4', '9.1.0.6', '9.0.4.4', '9.2.0.0',
                   '9.2.7', '9.1.1', '9.1.0.7', '9.0.6.1', '9.1.0.9', '3.0.0.3',
                   '9.1.0.1', '3.0.0.1', '3.0.0.0', '9.0.7.1', '9.0.9.0',
                   '9.0.7.3', '3.0.0.6', '3.0.0.2', '3.0.0.4', '3.0.0.5',
                   '1.0.0', ],
    "dev_type": ['win', 'mac'],
    "intv": ["1d", "1w", "1M"]
}


def parse_query_obj(q_obj):
    q_obj['start'] = q_obj.get("start", "2018-04-03")
    q_obj['end'] = q_obj.get("end", "2018-04-03")
    q_obj['intv'] = q_obj.get("intv", "1d")
    q_obj['dim'] = q_obj.get("dim", "dev_type")
    q_obj['condition'] = json.loads(q_obj.get('condition', "{}"))
    q_obj['page'] = int(q_obj.get("page", '1'))
    q_obj['size'] = int(q_obj.get("size", '20'))
    return q_obj


def gen_sql(sql, q_obj):
    """

    :param sql_str:  sql clause
    :param q_obj:  query  params objct
    :return:  result_sql
    """
    pp(q_obj)

    conditions = "__APP_VERSION__  __DEV_TYPE__ "
    replace_target = {'app_version': '', 'dev_type': ''}

    for k, v in replace_target.items():
        if k not in q_obj['condition']:
            v = ""
        else:
            v = "','".join(q_obj['condition'][k])
            print("the replace v is :",v)

        __replace__ = "__%s__" % k.upper()
        print(k, v, __replace__)
        if v:
            v = "AND %s in ('%s')" % (k, v)
            print("v:", v)
        conditions = conditions.replace(__replace__, v)



    print("conditions:", conditions)



    sql = sql.replace("__CONDITIONS__", conditions)
    sql = sql.replace("__START__", q_obj['start'])
    sql = sql.replace("__END__", q_obj['end'])
    sql = sql.replace("__DIM__", q_obj['dim'])

    return sql


@device_api.route("/device_option", methods=['GET'])
@swag_from('doc/device_option.yaml')
def device_option():
    return jsonify(options)


@device_api.route("/device_active_table", methods=['GET'])
@swag_from('doc/device_active_table.yaml')
def device_active_table():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    sql_str = """
    SELECT
	__DIM__,
	sum(increase) 
    FROM  __TABLE_NAME__
    where 	stat_date  BETWEEN '__START__' AND '__END__' 
    __CONDITIONS__
    GROUP BY	__DIM__  having  sum( increase )  >10 ;
    """
    sql = gen_sql(sql=sql_str, q_obj=q_obj)
    if q_obj['intv'] == '1d':
        sql = sql.replace('__TABLE_NAME__', 'device_active_day')
    elif q_obj['intv'] == '1w':
        sql = sql.replace('__TABLE_NAME__', 'device_active_week')
    else:
        sql = sql.replace('__TABLE_NAME__', 'device_active_month')
    print("active sql:", sql)

    host = settings.device_host
    port = settings.device_port
    user = settings.device_user
    password = settings.device_password
    db = 'data_user'
    device_mysql = pymysql.connect(host=host,
                                   user=user,
                                   password=password,
                                   db=db,
                                   port=port)

    with device_mysql.cursor() as cursor:
        # print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        final_result = []
        for idx, record in enumerate(record_list):
            # print("idx:", idx, '-->record', record)
            record_map = {}

            record_map[q_obj['dim']] = record[0]
            record_map['total_amount'] = int(record[1])
            # print("record_tmp:", record_map)
            final_result.append(record_map)

    # 当所选维度是日期时，针对日期排序，实现最近日期的数据显示在分页的靠前页面
    # print("un_sorted_final_result:",final_result)
    if 'stat_date' in final_result[0].keys():
        # print("begin to sort the result:")
        final_result = sorted(final_result, key=lambda e: e['stat_date'], reverse=True)
        # print("sorted_final_result:",final_result)

    # 添加数据分页功能
    paged_list = []
    start_idx, end_idx = q_obj['size'] * (q_obj['page'] - 1), q_obj['size'] * \
                         q_obj['page']
    for idx, result_one in enumerate(final_result):
        if start_idx <= idx < end_idx:
            paged_list.append(result_one)
    result = {
        'total': len(final_result),
        'result': paged_list
    }
    return jsonify(result)


@device_api.route("/device_increase_table", methods=['GET'])
@swag_from('doc/device_increase_table.yaml')
def device_increase_table():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    sql_str = """
        SELECT
        __DIM__ ,
        sum( increase ) 
        FROM  __TABLE_NAME__
        where 	stat_date  BETWEEN '__START__' AND '__END__' 
        __CONDITIONS__ 
        GROUP BY 	__DIM__  having  sum( increase )  >10 ;
        """
    sql = gen_sql(sql=sql_str, q_obj=q_obj)
    if q_obj['intv'] == '1d':
        sql = sql.replace('__TABLE_NAME__', 'device_increase_day')
    elif q_obj['intv'] == '1w':
        sql = sql.replace('__TABLE_NAME__', 'device_increase_week')
    else:
        sql = sql.replace('__TABLE_NAME__', 'device_increase_month')
    # print("increase sql:", sql)

    host = settings.device_host
    port = settings.device_port
    user = settings.device_user
    password = settings.device_password
    db = 'data_user'
    device_mysql = pymysql.connect(host=host,
                                   user=user,
                                   password=password,
                                   db=db,
                                   port=port)

    with device_mysql.cursor() as cursor:
        # print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        final_result = []
        for idx, record in enumerate(record_list):
            # print("idx:", idx, '-->record', record)
            record_map = {}

            record_map[q_obj['dim']] = record[0]
            record_map['total_amount'] = int(record[1])
            # print("record_tmp:", record_map)
            final_result.append(record_map)

    # 当所选维度是日期时，针对日期排序，实现最近日期的数据显示在分页的靠前页面
    # print("un_sorted_final_result:",final_result)
    if 'stat_date' in final_result[0].keys():
        # print("begin to sort the result:")
        final_result = sorted(final_result, key=lambda e: e['stat_date'], reverse=True)
        # print("sorted_final_result:",final_result)

    # 添加数据分页功能
    paged_list = []
    start_idx, end_idx = q_obj['size'] * (q_obj['page'] - 1), q_obj['size'] * \
                         q_obj['page']
    for idx, result_one in enumerate(final_result):
        if start_idx <= idx < end_idx:
            paged_list.append(result_one)
    result = {
        'total': len(final_result),
        'result': paged_list
    }
    return jsonify(result)
    return jsonify(result)


@device_api.route("/device_increase_graph", methods=['GET'])
@swag_from('doc/device_increase_graph.yaml')
def device_increase_graph():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    sql_str = """
        SELECT
        stat_date,
        __DIM__ ,
        sum( increase ) 
        FROM  __TABLE_NAME__
        where 	stat_date  BETWEEN '__START__' AND '__END__' 
        __CONDITIONS__ 
        GROUP BY stat_date, __DIM__  having  sum( increase )  >10 ;
        """
    sql = gen_sql(sql=sql_str, q_obj=q_obj)
    if q_obj['intv'] == '1d':
        sql = sql.replace('__TABLE_NAME__', 'device_increase_day')
    elif q_obj['intv'] == '1w':
        sql = sql.replace('__TABLE_NAME__', 'device_increase_week')
    else:
        sql = sql.replace('__TABLE_NAME__', 'device_increase_month')
    # print("increase sql:", sql)

    host = settings.device_host
    port = settings.device_port
    user = settings.device_user
    password = settings.device_password
    db = 'data_user'
    device_mysql = pymysql.connect(host=host,
                                   user=user,
                                   password=password,
                                   db=db,
                                   port=port)

    with device_mysql.cursor() as cursor:
        # print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        record_map = collections.OrderedDict()

        for record in record_list:
            if record[0] not in record_map:
                record_map[record[0]] = {"time": record[0], "buckets": []}
            record_map[record[0]]["buckets"].append({
                "dim": record[1],
                "total_amount": int(record[2])
            })
        # print("record_map:",record_map)
        result = [v for k, v in record_map.items()]
        # print("result:",result)
    return jsonify(result)


@device_api.route("/device_active_graph", methods=['GET'])
@swag_from('doc/device_active_graph.yaml')
def device_active_graph():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    sql_str = """
        SELECT
        stat_date,
        __DIM__ ,
        sum( increase ) 
        FROM  __TABLE_NAME__
        where 	stat_date  BETWEEN '__START__' AND '__END__' 
        __CONDITIONS__ 
        GROUP BY stat_date, __DIM__  having  sum( increase )  >10 ;
        """
    sql = gen_sql(sql=sql_str, q_obj=q_obj)
    if q_obj['intv'] == '1d':
        sql = sql.replace('__TABLE_NAME__', 'device_active_day')
    elif q_obj['intv'] == '1w':
        sql = sql.replace('__TABLE_NAME__', 'device_active_week')
    else:
        sql = sql.replace('__TABLE_NAME__', 'device_active_month')
    # print("active  sql:", sql)

    host = settings.device_host
    port = settings.device_port
    user = settings.device_user
    password = settings.device_password
    db = 'data_user'
    device_mysql = pymysql.connect(host=host,
                                   user=user,
                                   password=password,
                                   db=db,
                                   port=port)

    with device_mysql.cursor() as cursor:
        # print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        record_map = collections.OrderedDict()

        for record in record_list:
            if record[0] not in record_map:
                record_map[record[0]] = {"time": record[0], "buckets": []}
            record_map[record[0]]["buckets"].append({
                "dim": record[1],
                "total_amount": int(record[2])
            })
        # print("record_map:",record_map)
        result = [v for k, v in record_map.items()]
        # print("result:",result)

    return jsonify(result)


@device_api.route("/device_total", methods=['GET'])
@swag_from('doc/device_total.yaml')
def device_total():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    sql_str = """
         SELECT
        __DIM__ ,
        sum( total_amount ) 
        FROM  device_total_day
        where 	stat_date  BETWEEN '__START__' AND '__END__' 
        __CONDITIONS__ 
        GROUP BY __DIM__ HAVING sum( total_amount )  >10 ;
    """
    sql = gen_sql(sql=sql_str, q_obj=q_obj)
    # print("total sql:", sql)

    host = settings.device_host
    port = settings.device_port
    user = settings.device_user
    password = settings.device_password
    db = 'data_user'
    device_mysql = pymysql.connect(host=host,
                                   user=user,
                                   password=password,
                                   db=db,
                                   port=port)

    with device_mysql.cursor() as cursor:
        # print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        result = []
        for idx, record in enumerate(record_list):
            # print("idx:", idx, '-->record', record)
            record_map = {}

            record_map[q_obj['dim']] = record[0]

            record_map['total_amount'] = int(record[1])
            # print("record_tmp:", record_map)
            result.append(record_map)
        # col_name =[q_obj['dim'],'total_amount']
        # result = pd.DataFrame(list(record_list),columns= col_name)
        # result['show_date'] = result['show_date'].apply((lambda x: x.strftime("%Y-%m-%d")))
    return jsonify(result)


@device_api.route("/device_rention", methods=['GET'])
@swag_from('doc/device_rention.yaml')
def device_rention():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    sql_query = gen_sql(q_obj)
    date_histograms = query_ci_data(sql_query, q_obj)
    return jsonify(date_histograms)
