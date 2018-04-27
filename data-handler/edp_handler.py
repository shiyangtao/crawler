# -*- coding: utf-8 -*-

import sys
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')

import time
import json
import MySQLdb as db


warehouse_names = {
    '金源店': 103,
    '鸿坤店': 104,
    '回龙观华联店': 105,
    '福茂店': 106,
    '微信推广': 0,
    '': 0,
    '-': 0,
}

coupon_names = {
    '20元代金券': 4,
    '星座半价菜': 2,
    '海鲜长寿面一份': 1,
    '关注微信享会员特权': None,
    '50元抚慰券': None,
    '30元代金券': None,
    '30元代金礼券': None,
    '心动会员价': None,
    '咖喱阿根廷红虾5只': None,
    '100元霸王餐券': None,
    '': None,
    '-': None,
}

coupon_types = {
    '发放': 'SENT',
    '消费': 'CONSUME',
    '奖励': 'REWARD',
    '撤销奖励': 'CANCEL_REWARD',
    '撤销消费': 'CANCEL_CONSUME',
    '代金券过期': 'EXPIRE',
    '礼品券过期': 'EXPIRE',
    '': None,
    '-': None,
}

balance_types = {
    '充值': 'DEPOSIT',
    '消费': 'CONSUME',
    '手工增加': 'ADD',
    '手工扣减': 'REDUCE',
    '撤销充值': 'CANCEL_DEPOSIT',
    '撤销消费': 'CANCEL_CONSUME',
    '': None,
    '-': None,
}

points_types = {
    '消费奖励': 'REWARD',
    '撤销奖励': 'CANCEL_REWARD',
    '积分抵扣': 'DEDUCT',
    '积分消费撤销': 'CANCEL_DEDUCT',
    '手工增加': 'ADD',
    '手工扣减': 'REDUCE',
    '': None,
    '-': None,
}

input_dir = '/data/logs/scrapyd'
input_edp_user = '%s/EdpUserItem.json' % input_dir
input_edp_coupons = '%s/EdpCouponItem.json' % input_dir
input_edp_payment_audit = '%s/EdpBalanceAuditItem.json' % input_dir
input_edp_points_audit = '%s/EdpPointsAuditItem.json' % input_dir
input_edp_coupon_audit = '%s/EdpCouponAuditItem.json' % input_dir


def doWork(cursor):

    # writeEdpUser(cursor)
    # writeEdpCoupons(cursor)
    writeEdpPaymentAudit(cursor)
    writeEdpPointsAudit(cursor)
    writeEdpCouponAudit(cursor)


def writeEdpUser(cursor):

    with open(input_edp_user) as f:
        for line in f:
            line = line.strip()
            if line == '':
                continue

            item = json.loads(line)
            # print 'edp_user:', item

            item['birthday'] = getDate(item['birthday'])

            warehouse_id = 0
            warehouse_name = str(item['reg_warehouse'])
            if item['reg_warehouse'] is not None and warehouse_name in warehouse_names:
                warehouse_id = warehouse_names[warehouse_name]

            cursor.execute("insert into edp_user(id,uid,card_no,name,title,phone,balance,balance_in,points,points_in,level,birthday,reg_time,reg_warehouse,bound_to,coupon_num,warehouse_id) "
                           "values(NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s);",
                           (item['uid'], item['card_no'], item['name'], item['title'], item['phone'], item['balance'], item['balance_in'], item['points'], item['points_in'], item['level'], item['birthday'], item['reg_time'], item['reg_warehouse'], item['coupon_num'], warehouse_id)
                           )


def writeEdpCoupons(cursor):

    with open(input_edp_coupons) as f:
        for line in f:
            line = line.strip()
            if line == '':
                continue

            item = json.loads(line)
            # print 'edp_coupons:', item

            if item['name'] is None:
                continue

            coupon_id = None
            coupon_name = str(item['name'])
            if coupon_name in coupon_names:
                coupon_id = coupon_names[coupon_name]
            if coupon_id is None:
                continue

            cursor.execute("insert into edp_coupons(id,card_no,name,quantity,sent_time,expire_at,coupon_id) "
                           "values(NULL,%s,%s,%s,%s,%s,%s);",
                           (item['card_no'], item['name'], item['quantity'], item['sent_time'], item['expire_at'], coupon_id)
                           )


def writeEdpPaymentAudit(cursor):

    with open(input_edp_payment_audit) as f:
        for line in f:
            line = line.strip()
            if line == '':
                continue

            item = json.loads(line)
            # print 'edp_payment_audit:', item

            warehouse_id = 0
            warehouse_name = str(item['warehouse'])
            if item['warehouse'] is not None and warehouse_name in warehouse_names:
                warehouse_id = warehouse_names[warehouse_name]

            balance_type = balance_types[str(item['type'])]
            if balance_type is None:
                continue

            cursor.execute("insert into edp_payment_audit(id,card_no,type,time,warehouse,amount,reward,operator,warehouse_id) "
                           "values(NULL,%s,%s,%s,%s,%s,%s,%s,%s);",
                           (item['card_no'], balance_type, item['time'], item['warehouse'], item['amount'], item['reward'], item['operator'], warehouse_id)
                           )


def writeEdpPointsAudit(cursor):

    with open(input_edp_points_audit) as f:
        for line in f:
            line = line.strip()
            if line == '':
                continue

            item = json.loads(line)
            # print 'edp_points_audit:', item

            warehouse_id = 0
            warehouse_name = str(item['warehouse'])
            if item['warehouse'] is not None and warehouse_name in warehouse_names:
                warehouse_id = warehouse_names[warehouse_name]

            points_type = points_types[str(item['type'])]
            if points_type is None:
                continue

            cursor.execute("insert into edp_points_audit(id,card_no,type,time,warehouse,amount,operator,warehouse_id) "
                           "values(NULL,%s,%s,%s,%s,%s,%s,%s);",
                           (item['card_no'], points_type, item['time'], item['warehouse'], item['amount'], item['operator'], warehouse_id)
                           )


def writeEdpCouponAudit(cursor):

    with open(input_edp_coupon_audit) as f:
        for line in f:
            line = line.strip()
            if line == '':
                continue

            item = json.loads(line)
            # print 'edp_coupon_audit:', item

            warehouse_id = 0
            warehouse_name = str(item['warehouse'])
            if item['warehouse'] is not None and warehouse_name in warehouse_names:
                warehouse_id = warehouse_names[warehouse_name]

            coupon_id = None
            coupon_name = str(item['coupon_name'])
            if coupon_name in coupon_names:
                coupon_id = coupon_names[coupon_name]
            if coupon_id is None:
                continue

            coupon_type_id = None
            coupon_type = str(item['type'])
            if coupon_type in coupon_types:
                coupon_type_id = coupon_types[coupon_type]
            if coupon_type_id is None:
                continue

            cursor.execute("insert into edp_coupon_audit(id,card_no,type,time,warehouse,coupon_name,quantity,operator,warehouse_id,coupon_id) "
                           "values(NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                           (item['card_no'], coupon_type_id, item['time'], item['warehouse'], item['coupon_name'], item['quantity'], item['operator'], warehouse_id, coupon_id)
                           )


def withConnection(f):
    conn = None
    try:
        conn = db.connect(
            host="db-master.kuaihaowei.domain",
            port=13476,
            user="khw-usr",
            passwd="g5agXQwFkp3fD9US",
            db="khw",
            connect_timeout=5,
            use_unicode=True,
            charset='utf8',
            init_command='SET NAMES UTF8'
        )

        # conn = db.connect(
        #     host="127.0.0.1",
        #     port=3306,
        #     user="root",
        #     passwd="root",
        #     db="khw",
        #     connect_timeout=5,
        #     use_unicode=True,
        #     charset='utf8',
        #     init_command='SET NAMES UTF8'
        # )

        cursor = conn.cursor()

        f(cursor)

        cursor.close()
    finally:
        if conn:
            conn.commit()
            conn.close()


def getDate(str):
    if str == '':
        str = None

    if str is not None and not is_valid_date(str):
        str = None

    return str


def is_valid_date(str):
    try:
        time.strptime(str, "%Y-%m-%d")
        return True
    except:
        return False


if __name__ == '__main__':
    withConnection(doWork)
