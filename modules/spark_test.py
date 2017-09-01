#!/usr/bin/env python
# encoding:utf8

import os
import sys
sys.path.append('.')
import spark_lib
import json
import urlparse
from util import URLUtil
from operator import add
from pyspark.mllib.recommendation import Rating

PERM_PATH = '/groups/ddclick/perm_cust/result'
PROD_CORE = '/groups/reco/reco_job_base_data/put_files_position/base_products_core_insell.dat'
PROD_CATE = '/groups/reco/reco_job_base_data/put_files_position/base_category.dat'
CLICK_DAILY_PATH = '/groups/reco/click_daily_pull/{date:-1:-90:-}'
HDFS_PATH = '/groups/reco/reco_arch/filter_layer/personal_cate_reco/test_a/result_data'
# HDFS_PATH = '/personal/yuanyuan/update_user_label_to_es/result_data'
PC_CLICK_PATH = '/share/comm/ddclick/{date:-1:-90:-}/ddclick_product/'
APP_CLICK_PATH = '/share/comm/kafka/client/{date:-1:-90:-}/*'
H5_CLICK_PATH = '/share/comm/kafka/wap/{date:-1:-90:-}/*'
SAVE_HDFS = '/personal/yuanyuan/test/perm'


class A(object):
    sc = None
    main_cfg = None

    @classmethod
    def init(cls, main_cfg, sc):
        cls.main_cfg = main_cfg
        cls.sc = sc

    @staticmethod
    def add(x):
        return x+2

    @staticmethod
    def parse_reco(line):
        try:
            reco = json.loads(line.strip())
            # recos = json.loads(info)
        except Exception:
            return ()
        if '50244718' not in reco['uid'] and '50104929' not in reco['uid']:
            return None
        return line

    @staticmethod
    def parse_cate(line):
        items = line.strip().split('\t')
        cpath = items[0].encode('utf8').strip()
        cname = items[1].encode('utf8').strip()
        cid = int(items[2])
        if not cpath.startswith('01.41'):
            return ()
        return (cid, (cname, cpath))

    @staticmethod
    def parse_insell(line):
        try:
            items = line.strip().split('\t')
            pid = int(items[0])
            cpath = items[1].encode('utf8').strip()
            cid = int(items[2])
            sid = int(items[3])
        except Exception:
            return ()
        '''
        if pid != 1266842750:
            return ()
        return None
        '''
        if not cpath.startswith('01.41'):
            return ()
        if sid != 0:
            cpath = 'shop_%s' % cpath

        return ((cpath,cid), 1)

    @staticmethod
    def parse_perm(line):
        tokens = line.encode('utf8').strip().split('\t')
        perm = tokens[0]
        if len(perm) == 35:
            return (perm, 1)
        return ()

    @staticmethod
    def parse_personal(line):
        tokens = line.encode('utf8').strip().split('\t')
        perm = tokens[0]
        if len(perm) == 35:
            return (perm, 1)
        return ()

    @staticmethod
    def parse_click_daily(line):
        tokens = line.strip().split('\t')
        perm = tokens[0]
        cust = tokens[1]

        '''
        if cust:
            return ()
        '''
        if len(perm) == 35:
            return (perm, 1)
        return ()

    @staticmethod
    def parse_pc_click(line):
        tokens = line.strip().split('\001')
        perm = tokens[7]
        cust = tokens[6]

        if len(perm) != 35:
            return ''
        perm_set = PERM_SET.value
        if perm not in perm_set:
            return ''
        return (perm, 1)

    @staticmethod
    def parse_app_click(line):
        tokens = line.strip().split('\t')
        if len(tokens) < 18:
            return ''
        perm = tokens[17]
        cust = tokens[3]
        if len(perm) != 35:
            return ''

        perm_set = PERM_SET.value
        if perm not in perm_set:
            return ''
        return (perm, 1)

    @staticmethod
    def parse_h5_click(line):
        tokens = line.strip().split('\t')
        if len(tokens) < 20:
            return ''
        perm = tokens[16]
        cust = tokens[19]
        if len(perm) != 35:
            return ''

        perm_set = PERM_SET.value
        if perm not in perm_set:
            return ''

        return (perm, 1)

    @staticmethod
    def filter_perm(item):
        key, value = item
        if value[1] == 1:
            return ''
        return key

    @staticmethod
    def parse_uv_result(line):
        from pyspark.mllib.recommendation import Rating
        tokens = line.strip().split('\t')
        uid, reco_str = tokens
        reco_list = ['%s:%s' % (x.product, x.rating) for x in eval(reco_str)]
        return (uid, '\t'.join(reco_list))

    @classmethod
    def test(cls):
        perm_rdd = spark_lib.spark_read_hdfs(cls.sc, PERM_PATH)\
                            .map(cls.parse_perm).filter(None)

        '''
        click_perm = spark_lib.spark_read_hdfs(cls.sc, CLICK_DAILY_PATH)\
                              .map(cls.parse_click_daily).filter(None).distinct()\
                              .leftOuterJoin(perm_rdd)\
                              .map(cls.filter_perm).filter(None)\
                              .distinct().count()
        print 'click perm num:', click_perm

        '''
        cate_name = spark_lib.spark_read_hdfs(cls.sc, HDFS_PATH)\
                             .map(cls.parse_personal).filter(None)\
                             .leftOuterJoin(perm_rdd).map(cls.filter_perm).filter(None).collect()

        print 'perm num:', len(cate_name)
        print 'perm num distinct:', len(set(cate_name))
        global PERM_SET
        PERM_SET = cls.sc.broadcast(set(cate_name))

        '''
        all_dict = spark_lib.spark_read_hdfs(cls.sc, PC_CLICK_PATH)\
                            .map(cls.parse_pc_click).filter(None)\
                            .reduceByKey(add).count()

        print 'pc num:', all_dict
        all_dict = spark_lib.spark_read_hdfs(cls.sc, APP_CLICK_PATH)\
                            .map(cls.parse_app_click).filter(None)\
                            .reduceByKey(add).count()

        print 'app num:', all_dict
        '''
        all_dict = spark_lib.spark_read_hdfs(cls.sc, H5_CLICK_PATH)\
                            .map(cls.parse_h5_click).filter(None)\
                            .coleasce(1200).reduceByKey(add).count()

        print 'h5 num:', all_dict
        return
        # print cate_name
        # print all_dict
        shop_dict = {cid:num for (_,cid), num in all_dict if _.startswith('shop')}
        self_dict = {cid:num for (_,cid), num in all_dict if not _.startswith('shop')}

        result_lst = []
        for cid in cate_name:
            cname, cpath = cate_name[cid]
            shop_num = shop_dict.get(cid, 0)
            self_num = self_dict.get(cid, 0)
            value = [cpath, str(cid), cname, str(self_num), str(shop_num)]
            result_lst.append('\t'.join(map(str, value)))

        result_lst.sort()
        with open('1.txt', 'w') as fp_w:
            for line in result_lst:
                print>>fp_w, line
        '''
        new_rdd = spark_lib.spark_read_hdfs(cls.sc, HDFS_PATH)\
                           .map(cls.parse_reco).filter(None)
        print 'target line:', new_rdd.take(10)
        # spark_lib.save_rdd_to_hdfs(new_rdd, SAVE_HDFS, outformat)
        '''
        return


def main():
    from common.spark_init import init_spark
    from ConfigParser import SafeConfigParser

    main_cfg = SafeConfigParser()
    if os.path.exists('conf/main.cfg'):
        main_cfg.read('conf/main.cfg')
    else:
        main_cfg.read('main.cfg')
    A.init(main_cfg, init_spark(main_cfg))

if __name__ == '__main__':
    main()
