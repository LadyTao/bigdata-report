import pymysql
import json


def ci_channel_info(host, port, user, password, db, charset='utf8mb4'):
    """
    用于获取神剪手后台数据库的渠道分类信息，同时在叶子节点添加相应的渠道名称
    :param host:
    :param port:
    :param user:
    :param password:
    :param db:
    :param charset:
    :return:
    """

    ci_channel = pymysql.connect(host=host, port=port, user=user,
                                 password=password, db=db,
                                 charset=charset,

                                 cursorclass=pymysql.cursors.SSDictCursor)
    # 获取叶子节点和对应的渠道名称
    channel_name = {}
    with ci_channel.cursor() as cursor:
        sql_channel_name = """
          SELECT t4.catname,
        t7.title
        from 
        (SELECT catname,id ,parentid from wx_category where taxonomy='marketing_channel'  and child=0) t4
        LEFT JOIN 
          ( SELECT term_id, post_id, post_type FROM wx_category_posts WHERE post_type = 'marketing_channel' ) t6
          ON t4.id = t6.term_id
          LEFT JOIN ( SELECT id, title, product_id FROM wx_marketing_channel ) t7 ON t6.post_id = t7.id;
                
        """
        # print(sql_channel_name)
        cursor.execute(sql_channel_name)
        record_list = cursor.fetchall()
        for record in record_list:
            channel_name[record["catname"]] = record["title"]

    channel_level = {}
    with ci_channel.cursor() as cursor:
        sql_channel_level = """
          SELECT
          -- t1.id, 
          t1.catname as channel_1,
          -- t2.id,
          -- t2.parentid,
          t2.catname as channel_2,
          -- t3.id, 
          -- t3.parentid, 
          t3.catname as channel_3

          from 
          (SELECT id,catname from wx_category where taxonomy='marketing_channel' and parentid=0) t1
           left join 
          (SELECT catname,id ,parentid from wx_category where taxonomy='marketing_channel' and parentid in (SELECT id from wx_category where taxonomy='marketing_channel' and parentid=0)) t2 
          on t1.id = t2.parentid
          LEFT JOIN
          (select t4.id,t4.parentid,t4.catname from 
          (SELECT catname,id ,parentid from wx_category where taxonomy='marketing_channel'  and child=0) t4  
          inner JOIN
          (SELECT catname,id ,parentid from wx_category where taxonomy='marketing_channel' and parentid in (SELECT id from wx_category where taxonomy='marketing_channel' and parentid=0)) t5  
          on t5.id = t4.parentid) t3
          on t2.id = t3.parentid;
        """
        # print(sql_channel_level)
        cursor.execute(sql_channel_level)
        record_list = cursor.fetchall()
        # print(record_list)
        from collections import defaultdict
        channel_level = defaultdict(
            lambda: defaultdict(  # lv1
                lambda: defaultdict(  # lv2
                    lambda: defaultdict(dict))))  # lv3
        for id, record in enumerate(record_list):
            lv1, lv2, lv3 = record['channel_1'], record['channel_2'], record[
                'channel_3']
            if lv3 is not None:
                if channel_name[lv3]:
                    channel_level[lv1][lv2][lv3] = channel_name[lv3]
            if lv3 is None:
                if channel_name[lv2]:
                    channel_level[lv1][lv2] = channel_name[lv2]
        channel_level["其他"] = 'missing'
        channel_level = json.loads(
            json.dumps(channel_level, ensure_ascii=False))
    return channel_name, channel_level