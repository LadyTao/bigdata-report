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

cart_api = Blueprint('cart_api', __name__)

import pymysql.cursors
import settings

db = settings.device_db


def parse_query_obj(q_obj):
    q_obj['start'] = q_obj.get("start", "2018-04-03")
    q_obj['end'] = q_obj.get("end", "2018-04-03")
    q_obj['intv'] = q_obj.get("intv", "1d")
    q_obj['dim'] = q_obj.get("dim", "dev_type")
    q_obj['funnel_type'] = q_obj.get("funnel_type", "pay_ok")
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

    conditions = "__PRODUCT__  __SITE_TYPE__ "
    replace_target = {'product': '', 'site_type': ''}

    for k, v in replace_target.items():
        if k not in q_obj['condition']:
            v = ""
        else:
            v = "','".join(q_obj['condition'][k])
            # print("the replace v is :", v)

        __replace__ = "__%s__" % k.upper()
        # print(k, v, __replace__)
        if v:
            v = "AND %s in ('%s')" % (k, v)
            # print("v:", v)
        conditions = conditions.replace(__replace__, v)

    print("conditions:", conditions)

    sql = sql.replace("__CONDITIONS__", conditions)
    sql = sql.replace("__START__", q_obj['start'])
    sql = sql.replace("__END__", q_obj['end'])
    sql = sql.replace("__DIM__", q_obj['dim'])

    return sql


@cart_api.route("/cart_funnel_table", methods=['GET'])
@swag_from('doc/cart_funnel_table.yaml')
def cart_funnel_table():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    sql_str = """
                SELECT
                __DIM__,
                sum(plan_num)  as plan_num,
				sum(plan_user_num)  as plan_user_num,
				sum(checkout_num)  as checkout_num,
				sum(checkout_user_num)  as checkout_user_num,
				sum(generate_num)  as generate_num,
				sum(succeed_num)  as succeed_num,
				sum(falied_num)  as falied_num,
				sum(unpay_num)  as unpay_num,
				sum(checkout_user_num)/sum(plan_user_num) as plan_user_rate ,
				sum(succeed_num)/sum(checkout_user_num) as user_buy_rate ,
				sum(generate_num)/sum(checkout_user_num) as user_commit_rate,
				sum(succeed_num)/(sum(succeed_num)+sum(falied_num)) as pay_succeed_rate,
				sum(succeed_num)/sum(generate_num) as pay_convert_rate
                FROM  __TABLE_NAME__
                where 	stat_date  BETWEEN '__START__' AND '__END__' 
                __CONDITIONS__ 
                GROUP BY __DIM__ ;
                """
    sql = gen_sql(sql=sql_str, q_obj=q_obj)
    if q_obj['intv'] == '1d':
        sql = sql.replace('__TABLE_NAME__', 'funnel_shopcart_day')
    elif q_obj['intv'] == '1w':
        sql = sql.replace('__TABLE_NAME__', 'funnel_shopcart_week')
    else:
        sql = sql.replace('__TABLE_NAME__', 'funnel_shopcart_month')
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

    record_map = collections.OrderedDict()
    with device_mysql.cursor() as cursor:
        print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        for record in record_list:
            # print("record:", record)
            tmp = collections.OrderedDict()
            tmp["plan_num"] = int(record[1])  # 进入plan页面次数
            tmp["plan_user_num"] = int(record[2])  # 进入plan页用户数
            tmp["checkout_num"] = int(record[3])  # 进入chekout页面次数
            tmp["checkout_user_num"] = int(record[4])  # 进入checkout页面用户数
            tmp["generate_num"] = int(record[5])  # 生成订单数
            tmp["succeed_num"] = int(record[6])  # 成功支付订单数
            tmp["falied_num"] = int(record[7])  # 失败支付订单数
            tmp["unpay_num"] = int(record[8])  # 未支付订单数
            tmp["plan_user_rate"] = float(record[9])
            tmp["user_buy_rate"] = float(record[10])
            tmp["user_commit_rate"] = float(record[11])
            tmp["pay_succeed_rate"] = float(record[12])  # 支付成功率:成功支付订单数/（成功订单数+
            tmp["pay_convert_rate"] = float(record[13])  # 支付转化率:成功订单/生成订单数

            record_map[record[0]] = tmp
    print("record_tmp:", record_map)
    record_map = [(k, v) for k, v in sorted(record_map.items(), reverse=True)]
    print("the length of record_tmp:", len(record_map))

    # 添加数据分页功能
    paged_list = []
    start_idx, end_idx = q_obj['size'] * (q_obj['page'] - 1), q_obj['size'] * \
                         q_obj['page']
    for idx, result_one in enumerate(record_map):
        if start_idx <= idx < end_idx:
            paged_list.append(result_one)
    page_result = {
        'total': len(record_map),
        'result': paged_list
    }

    # 返回分页数据
    page_result = {"code": 200,
                   "msg": "成功",
                   "data": page_result}
    # print("result", result)
    return jsonify(page_result)


@cart_api.route("/cart_funnel_graph", methods=['GET'])
@swag_from('doc/cart_funnel_graph.yaml')
def cart_funnel_graph():
    """
    plan页用户转化率:(加入购物车单数/Plan用户数) plan_user_rate
    用户购买转化率:(支付成功单数 /加入购物车单数) user_buy_rate
    用户支付提交率:(生成订单数/加入购物车单数)  user_commit_rate
    支付成功率:(成功支付订单数/（成功订单数+失败订单数）) pay_succeed_rate
    支付转化率:(成功订单/生成订单数)   pay_convert_rate

    :return:
    """
    sql_map = {
        "pay_succeed_rate": "sum(succeed_num)/(sum(succeed_num)+sum(falied_num)) as pay_succeed_rate ",
        "pay_convert_rate": "sum(succeed_num)/sum(generate_num) as pay_convert_rate ",
        "plan_user_rate": "sum(checkout_user_num)/sum(plan_user_num) as plan_user_rate ",
        "user_buy_rate": "sum(succeed_num)/sum(checkout_user_num) as user_buy_rate ",
        "user_commit_rate": "sum(generate_num)/sum(checkout_user_num) as user_commit_rate "
    }
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    sql_str = """
            SELECT
            stat_date,
            sum(__DIM__)  as dim
            FROM  __TABLE_NAME__
            where 	stat_date  BETWEEN '__START__' AND '__END__' 
            __CONDITIONS__ 
            GROUP BY stat_date ;
            """
    if q_obj['dim'] == "pay_succeed_rate":
        sql_str = sql_str.replace("sum(__DIM__)  as dim", sql_map["pay_succeed_rate"])
    elif q_obj['dim'] == "pay_convert_rate":
        sql_str = sql_str.replace("sum(__DIM__)  as dim", sql_map["pay_convert_rate"])
    elif q_obj['dim'] == "plan_user_rate":
        sql_str = sql_str.replace("sum(__DIM__)  as dim", sql_map["plan_user_rate"])

    elif q_obj['dim'] == "user_buy_rate":
        sql_str = sql_str.replace("sum(__DIM__)  as dim", sql_map["user_buy_rate"])

    elif q_obj['dim'] == "user_commit_rate":
        sql_str = sql_str.replace("sum(__DIM__)  as dim", sql_map["user_commit_rate"])

    sql = gen_sql(sql=sql_str, q_obj=q_obj)
    if q_obj['intv'] == '1d':
        sql = sql.replace('__TABLE_NAME__', 'funnel_shopcart_day')
    elif q_obj['intv'] == '1w':
        sql = sql.replace('__TABLE_NAME__', 'funnel_shopcart_week')
    else:
        sql = sql.replace('__TABLE_NAME__', 'funnel_shopcart_month')
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

    record_map = {}
    with device_mysql.cursor() as cursor:
        print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        for record in record_list:
            if q_obj['dim'] in ["pay_succeed_rate", "pay_convert_rate", "plan_user_rate", "user_buy_rate",
                                "user_commit_rate"]:
                # print("type:", type(float(record[1])))
                record_map[record[0]] = float(record[1])
            else:
                # print("type:", type(record[1]))
                record_map[record[0]] = int(record[1])

    result = {"code": 200,
              "msg": "成功",
              "data": record_map}
    print("result", result)
    return jsonify(result)


@cart_api.route("/cart_funnel", methods=['GET'])
@swag_from('doc/cart_funnel.yaml')
def cart_funnel():
    """
    购物车漏斗类型：购物车支付成功转化漏斗，购物车支付失败漏斗，未支付购物车漏斗

    :return:
    """
    sql_map = {
        "unpay": """
        sum(unpay_num)  as unpay_num,
	    sum(unpay_num)/sum(plan_user_num) as unpay_num
        """,
        "pay_ok": """
        sum(succeed_num)  as succeed_num,
				sum(succeed_num)/sum(plan_user_num) as succeed_num_rate
        """,
        "pay_fail": """
            sum(falied_num)  as falied_num,
				sum(falied_num)/sum(plan_user_num) as falied_num_rate
        """,

    }
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)

    sql_str = """
            SELECT
				sum(plan_user_num)  as plan_user_num,
				sum(checkout_user_num)  as checkout_user_num,
				sum(checkout_user_num)/sum(plan_user_num) as checkout_rate ,
				sum(generate_num)  as generate_num,
				sum(generate_num)/sum(plan_user_num) as generate_num_rate,
		        __REPLACE_SUM__
            FROM  funnel_shopcart_day
            where 	stat_date  BETWEEN '__START__' AND '__END__' 
            __CONDITIONS__ 
            GROUP BY stat_date ;
       
         """
    #  [pay_ok,pay_fail,unpay]
    if q_obj['funnel_type'] == "pay_ok":
        sql_str = sql_str.replace("__REPLACE_SUM__", sql_map["pay_ok"])
    elif q_obj['funnel_type'] == "pay_fail":
        sql_str = sql_str.replace("__REPLACE_SUM__", sql_map["pay_fail"])
    elif q_obj['funnel_type'] == "unpay":
        sql_str = sql_str.replace("__REPLACE_SUM__", sql_map["unpay"])

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

    record_map = collections.OrderedDict()
    with device_mysql.cursor() as cursor:
        print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        # print(record_list)

        for record in record_list:
            record_map["plan_user_num"] = int(record[0])
            record_map["checkout_user_num"] = int(record[1])
            record_map["checkout_rate"] = float(record[2])
            record_map["generate_num"] = int(record[3])
            record_map["generate_num_rate"] = float(record[4])
            record_map["final_user"] = int(record[5])
            record_map["final_rate"] = float(record[6])

    print("record_map:", record_map)
    result = {"code": 200,
              "msg": "成功",
              "data": record_map}
    print("result", result)
    return jsonify(result)


@cart_api.route("/cart_option", methods=['GET'])
@swag_from('doc/cart_option.yaml')
def cart_option():
    sql = 'SELECT  DISTINCT product, site_type from funnel_shopcart_day'

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
        print("sql_query in get data:", sql)
        cursor.execute(sql)
        record_list = cursor.fetchall()
        for idx, record in enumerate(record_list):
            print("record:", record)
            if record[0] not in options.keys():
                options[record[0]] = []
                options[record[0]].append(record[1])
            else:
                options[record[0]].append(record[1])

    print("options:", options)
    fianal = {"product": [k for k, v in options.items()],
              "site_type": options}

    result = {"code": 200,
              "msg": "成功",
              "data": fianal}

    return jsonify(result)
