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
            print("the replace v is :", v)

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
    # 查询数据库获取相关的设备类型下拉项和其对应的子选项

    sql = 'SELECT  DISTINCT dev_type, app_version from device_active_day'

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

    options = collections.defaultdict()

    with device_mysql.cursor() as cursor:
        # print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        for idx, record in enumerate(record_list):
            # print("record:", record)
            if record[0] not in options.keys():
                options[record[0]] = []
                options[record[0]].append(record[1])
            else:
                options[record[0]].append(record[1])

    print("options:", options)
    result = {"dev_type": sorted([k for k, v in options.items()]),
              "app_version": options,
              "intv": ["1d", "1w", "1M"]}
    # result = {"code": 200,
    #           "msg": "成功",
    #           "data": fianal}


    return jsonify(result)


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
        print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        final_result = []
        for idx, record in enumerate(record_list):
            print("idx:", idx, '-->record', record)
            record_map = {}

            record_map[q_obj['dim']] = record[0]
            record_map['total_amount'] = int(record[1])
            print("record_tmp:", record_map)
            final_result.append(record_map)

    # 当所选维度是日期时，针对日期排序，实现最近日期的数据显示在分页的靠前页面
    print("un_sorted_final_result:", final_result)
    if 'stat_date' in final_result[0].keys():
        # print("begin to sort the result:")
        final_result = sorted(final_result, key=lambda e: e['stat_date'], reverse=True)
        print("sorted_final_result:", final_result)

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
        where 	total_amount>10
        __CONDITIONS__ 
        GROUP BY __DIM__ HAVING sum( total_amount )  >100 ;
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


@device_api.route("/device_rention_table", methods=['GET'])
@swag_from('doc/device_rention_table.yaml')
def device_rention_table():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    # 根据传入的时间间隔，查询不同的表，返回不同的数据
    if q_obj['intv'] == '1d':
        sql_str="""
        SELECT
        __DIM__,
        sum(increase_count),
        sum(one_day) ,
        sum(two_day),
        sum(three_day),
        sum(four_day),
        sum(five_day),
        sum(six_day),
        sum(seven_day),
        sum(one_day)/sum(increase_count) ,
        sum(two_day)/sum(increase_count),
        sum(three_day)/sum(increase_count),
        sum(four_day)/sum(increase_count),
        sum(five_day)/sum(increase_count),
        sum(six_day)/sum(increase_count),
        sum(seven_day)/sum(increase_count)
    FROM
        device_increase_retention_day 
    WHERE
          where 	stat_date  BETWEEN '__START__' AND '__END__' 
        __CONDITIONS__
        GROUP BY	__DIM__  ;
        
        """
            #sql = sql.replace('__TABLE_NAME__', 'device_active_day')
    elif q_obj['intv'] == '1w':
        sql_str="""
                SELECT
                __DIM__,
                sum(increase_count),
                sum(one_week) ,
                sum(two_week),
                sum(three_week),
                sum(four_week),
                sum(five_week),
                sum(six_week),
                sum(seven_week),
                sum(eight_week),
                sum(one_week)/sum(increase_count) ,
                sum(two_week)/sum(increase_count),
                sum(three_week)/sum(increase_count),
                sum(four_week)/sum(increase_count),
                sum(five_week)/sum(increase_count),
                sum(six_week)/sum(increase_count),
                sum(seven_week)/sum(increase_count),
                sum(eight_week)/sum(increase_count)
            FROM
                device_increase_retention_week 
            WHERE
                  where 	stat_date  BETWEEN '__START__' AND '__END__' 
                __CONDITIONS__
                GROUP BY	__DIM__  ;
                """
    #sql = sql.replace('__TABLE_NAME__', 'device_active_week')
    else:
        sql_str="""
                SELECT
                __DIM__,
                sum(increase_count),
                sum(one_month) ,
                sum(two_month),
                sum(three_month),
                sum(four_month),
                sum(five_month),
                sum(six_month),
                sum(seven_month),
                sum(eight_month),
                sum(one_month)/sum(increase_count) ,
                sum(two_month)/sum(increase_count),
                sum(three_month)/sum(increase_count),
                sum(four_month)/sum(increase_count),
                sum(five_month)/sum(increase_count),
                sum(six_month)/sum(increase_count),
                sum(seven_month)/sum(increase_count),
                sum(eight_month)/sum(increase_count)
            FROM
                device_increase_retention_month 
            WHERE
                  where 	stat_date  BETWEEN '__START__' AND '__END__' 
                __CONDITIONS__
                GROUP BY	__DIM__  ;
            """

    sql = gen_sql(sql=sql_str, q_obj=q_obj)
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
        print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        result = []
        for idx, record in enumerate(record_list):
            if q_obj["intv"]=='1d' and q_obj["retention_var"]=="retention_user":
                print("idx:", idx, '-->record', record)




            # print("record_tmp:", record_map)

        # col_name =[q_obj['dim'],'total_amount']
        # result = pd.DataFrame(list(record_list),columns= col_name)
        # result['show_date'] = result['show_date'].apply((lambda x: x.strftime("%Y-%m-%d")))
    return jsonify(result)
