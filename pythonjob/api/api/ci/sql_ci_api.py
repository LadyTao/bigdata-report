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
import settings
import json
from elasticsearch import Elasticsearch
from flask import Blueprint, render_template
from flasgger import swag_from

ci_api = Blueprint('ci_api', __name__)

from api.ci import channel_options
import pymysql.cursors
import settings

host = settings.ci_mysql_host
port = settings.ci_mysql_port
user = settings.ci_mysql_user
password = settings.ci_mysql_password
db = settings.ci_mysql_db

option = {
    "channel": ["百度推广", "胡萝卜周", "易企传", "搜狗推广", "360推广", "BD通用", "BD-XL",
                "BD-JJDS", "baidu-mobile", "BD-KSJKH", "baidu-zhishi",
                "baidu-pcinfo", "麦本本", "BD电商活动", "赢商荟", "智适应", "bili",
                "BZdownload", "依凰蓝盾", "常乐九九", "栗子摄影器材", "连锁正品店", "BZdownload2",
                "广点通推广", "优设网", "齐论电商", "金山毒霸", "腾讯管家", "360管家", "深圳高级中学",
                "搜狗WAP注册", "360WAP注册", "神马WAP注册", "10天试用-电商组自媒体", "百度搜索推广WAP端",
                "高校拓展", "搜狗WAP下载", "BD官网", "BD-视达", "神剪手BD微信社群新注册用户5天VIP",
                "今日头条IOS", "神剪手BD-QQ群李栋推广", "百度搜索品牌推广PC端", "微博红人推广（app）"],
    "productline": ["神剪手", "神剪手移动端", "其他"],
    "subscribe_type": ["月付", "年付", "其他"],
    "member_class": ["高级会员", "VIP至尊会员", "企业会员", "其他"],
    "os_platform": ["Windows", "Android", "IOS", "其他"],
    "payment_pattern": ["支付宝", "微信", "小程序", "IOS支付", "无需支付", "其他"],
    "intv": ["1d", "1w", "1M"]
}

channel_host = settings.ci_channel_host
channel_port = settings.ci_channel_port
channel_user = settings.ci_channel_user
channel_password = settings.ci_channel_password
channel_db = settings.ci_channel_db

_, level = channel_options.ci_channel_info(host=channel_host, port=channel_port,
                                           user=channel_user,
                                           password=channel_password,
                                           db=channel_db)
option["channel"] = level
options = option


# print("options:", options)


def parse_query_obj(q_obj):
    q_obj['start'] = q_obj.get("start", "2018-01-01")
    q_obj['end'] = q_obj.get("end", "2018-01-10")
    q_obj['dim'] = q_obj.get("dim", "inputtime")
    q_obj['intv'] = q_obj.get("intv", "1d")
    q_obj['condition'] = json.loads(q_obj.get('condition', "{}"))
    q_obj['page'] = int(q_obj.get("page", '1'))
    q_obj['size'] = int(q_obj.get("size", '20'))
    return q_obj


def gen_sql(q_obj):
    pp(q_obj)
    sql = """ 
           select 
           __INTV__ as intv, 
           __DIM__ as dim,
           sum(amount) as amount,
           sum(origin_amount) as origin_amount,
           count(distinct order_no) as order_counts,
           count(distinct uid) as user_counts
           from ci_order
           where
           __INTV__ between '__START__' and '__END__' 
           __CONDITIONS__
           group by intv, dim;
           """

    conditions = " __CHANNEL__  __PRODUCTLINE__  __SUBSCRIBE_TYPE__  __MEMBER_CLASS__  __OS_PLATFORM__  __PAYMENT_PATTERN__"
    replace_target = {'channel': '', 'subscribe_type': '', 'member_class': '',
                      'os_platform': '', 'payment_pattern': '',
                      'productline': ''}
    for k, v in replace_target.items():
        if k not in q_obj['condition']:
            v = ""
        else:
            v = "','".join(q_obj['condition'][k])
        __replace__ = "__%s__" % k.upper()
        print(k, v, __replace__)
        if v != '':
            v = " AND %s in ('%s')" % (k, v)
        conditions = conditions.replace(__replace__, v)

    intv = 'show_date'
    if q_obj['intv'] == '1M':
        intv = 'show_month'
    if q_obj['intv'] == '1w':
        intv = 'show_week'
    if q_obj['dim'] == 'inputtime':
        q_obj['dim'] = intv

    sql = sql.replace("__CONDITIONS__", conditions)
    sql = sql.replace("__START__", q_obj['start'])
    sql = sql.replace("__END__", q_obj['end'])
    sql = sql.replace("__DIM__", q_obj['dim'])
    sql = sql.replace("__INTV__", intv)

    return sql


def query_ci_data(sql_query, q_obj):
    ci_mysql = pymysql.connect(host=host, port=port, user=user,
                               password=password, db=db,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.SSDictCursor)

    result = []
    with ci_mysql.cursor() as cursor:

        print(sql_query)
        cursor.execute(sql_query)
        record_list = cursor.fetchall()
        """
        intv          dim       amount     order_counts user_counts
	2019-01-01    360推广    2652.58    60    51
	2019-01-01    BD电商活动    19.99    2    2
	2019-01-01    BZdownload    19.99    1    1
	2019-01-01    常乐九九    0.00    2    2
	2019-01-01    搜狗WAP下载    9.99    1    1
	2019-01-01    搜狗推广    1716.73    40    30
	2019-01-01    易企传    0.00    15    15
	2019-01-01    百度推广    17849.47    373    300
	2019-01-01    百度搜索推广WAP端    108.99    2    2
	2019-01-01    胡萝卜周    19.99    15    15
	2019-01-01    腾讯管家    935.81    23    15
	2019-01-01    金山毒霸    99.00    1    1
	2019-01-02    360推广    2752.55    65    54
	2019-01-02    BD电商活动    39.96    9    8
        """
        time_buckets_map = collections.OrderedDict()
        for record in record_list:
            intv = "%sT00:00:00.000+08:00" % record['intv']
            if intv not in time_buckets_map:
                time_buckets_map[intv] = {"time": intv, "buckets": []}
            dim = intv if q_obj["dim"] == "inputtime" else record["dim"]
            time_buckets_map[intv]["buckets"].append(
                {"dim": dim,
                 "amount": record["amount"],
                 "origin_amount": record["origin_amount"],
                 "order_counts": record["order_counts"],
                 "user_counts": record["user_counts"]})
        result = [v for k, v in time_buckets_map.items()]
    ci_mysql.close()
    return result


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
                dim_matrix_map[bucket['dim']] = {'user_counts': 0,
                                                 'order_counts': 0, 'amount': 0,
                                                 'origin_amount': 0}
            for key in bucket.keys():
                if key == 'dim':
                    continue
                dim_matrix_map[bucket['dim']][key] += bucket[key]

    sorted_dim_matrix_map = collections.OrderedDict(
        sorted(dim_matrix_map.items(), reverse=True))

    dim_matric_list = []
    for dim, matrics_map in sorted_dim_matrix_map.items():
        matrics_map['dim'] = dim
        matrics_map['amount'] = round(matrics_map['amount'], 2)
        matrics_map['origin_amount'] = round(matrics_map['origin_amount'], 2)
        dim_matric_list.append(matrics_map)

    return dim_matric_list


@ci_api.route("/ci_sales_option", methods=['GET'])
@swag_from('doc/ci_sales_option.yaml')
def ci_option():
    return jsonify(options)


@ci_api.route("/ci_sales_graph", methods=['GET'])
@swag_from('doc/ci_sales_graph.yaml')
def ci_sales_graph():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    sql_query = gen_sql(q_obj)
    date_histograms = query_ci_data(sql_query, q_obj)
    return jsonify(date_histograms)


@ci_api.route("/ci_sales_table", methods=['GET'])
@swag_from('doc/ci_sales_table.yaml')
def ci_sales_table():
    """
	{
	  "result": [
	    {
	      "amount": 29150.62,
	      "dim": "360推广",
	      "order_counts": 641,
	      "user_counts": 539
	    },
	    {
	      "amount": 0,
	      "dim": "高校拓展",
	      "order_counts": 1,
	      "user_counts": 1
	    }
	  ],
	  "total": 22
	}

    """
    # SQL-WAY
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    sql_query = gen_sql(q_obj)
    sql_query = sql_query.replace("group by intv,", "group by ")
    date_histograms = query_ci_data(sql_query, q_obj)
    dim_statistics_list = get_dim_statistics_list(date_histograms)
    paged_list = []
    start_idx, end_idx = q_obj['size'] * (q_obj['page'] - 1), q_obj['size'] * \
                         q_obj['page']

    # print("before sortting the dim_statistics_list:", dim_statistics_list)
    if q_obj['dim'] in ["show_date", "show_week", "show_month"]:
        dim_statistics_list = sorted(dim_statistics_list, key=lambda k: k['dim'], reverse=True)
    else:
        dim_statistics_list = sorted(dim_statistics_list, key=lambda k: k['user_counts'], reverse=True)
    #
    # print("sorted the dim_statistics_list:", dim_statistics_list)
    # print("the dim:", q_obj['dim'])

    for idx, dim_statistics in enumerate(dim_statistics_list):
        if start_idx <= idx < end_idx:
            paged_list.append(dim_statistics)
    result = {
        'total': len(dim_statistics_list),
        'result': paged_list
    }
    return jsonify(result)


@ci_api.route("/ci_sales_csv", methods=['GET'])
@swag_from('doc/ci_sales_csv.yaml')
def ci_sales_csv():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    sql_query = gen_sql(q_obj)
    sql_query = sql_query.replace("group by intv,", "group by ")
    date_histograms = query_ci_data(sql_query, q_obj)
    dim_statistics_list = get_dim_statistics_list(date_histograms)
    print("q_obj:", q_obj)

    def dict_to_str(dim, dim_statistics_list):
        field_mapping = {"show_week": "时间", "show_month": "时间", "show_date": "时间", "channel": "渠道",
                         "productline": "产品线",
                         "subscribe_type": "订阅类型", "member_class": "会员等级",
                         "os_platform": "操作系统", "payment_pattern": "支付方式"}
        import codecs
        yield codecs.BOM_UTF8
        # yield "%s,user_counts,order_counts,amount\n"%dim
        yield "%s,用户数,订单数,总金额\n" % field_mapping[dim]
        for dim_statistics in dim_statistics_list:
            yield "%s,%s,%s,%s\n" % (dim_statistics['dim'],
                                     dim_statistics['user_counts'],
                                     dim_statistics['order_counts'],
                                     dim_statistics['amount'])

    return Response(dict_to_str(q_obj['dim'], dim_statistics_list),
                    mimetype='text/csv')


@ci_api.route("/ci_sales_compare", methods=['GET'])
@swag_from('doc/ci_sales_compare.yaml')
def ci_sales_compare():
    q_obj = request.args.to_dict()
    q_obj = parse_query_obj(q_obj)
    sql_query = gen_sql(q_obj)
    date_histograms = query_ci_data(sql_query, q_obj)
    dim_statistics_list = get_dim_statistics_list(date_histograms)
    result = {
        'title': q_obj['title'],
        'amount': 0,
        'origin_amount': 0,
        'user_counts': 0,
        'order_counts': 0
    }
    for dim_statistics in dim_statistics_list:
        """
	{
	    "amount": 0,
	    "dim": "神剪手BD-QQ群李栋推广",
            "order_counts": 7,
	    "user_counts": 7
	},
        """
        for k, v in result.items():
            if k == 'title':
                continue
            result[k] += dim_statistics[k]
            result[k] = round(result[k], 2)

    return jsonify(result)


@ci_api.route("/board_notes", methods=['GET'])
@swag_from('doc/board_notes.yaml')
def board_notes():
    """
    :param note_id: default:1
    :return:  note_info:json字符串
    """

    """
    return example :{
	"first_note": {
		"default": "第一次启动应用的用户（以设备为判断标准），按周（按月）显示新增用户时，界面上以每周的周日（每个月的最后一日）来代表该周（该月）"
	},
	"second_note": {
		"default": "第一次启动应用的用户（以设备为判断标准），按周（按月）显示新增用户时，界面上以每周的周日（每个月的最后一日）来代表该周（该月）"
	}
}
    
    
    """
    q_obj = request.args.to_dict()
    note_id = int(q_obj["note_id"])
    query = 'select note_info from analysis_platform.page_note where note_id =%d;' % note_id
    db = settings.ci_notes_db

    ci_mysql = pymysql.connect(host=host, port=port, user=user,
                               password=password, db=db,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.SSDictCursor)

    with ci_mysql.cursor() as cursor:
        # print(query)
        cursor.execute(query)
        record_list = cursor.fetchall()
        result = eval(record_list[0]["note_info"])
        # print("tyep of result:",type(result))
    return jsonify(result)
