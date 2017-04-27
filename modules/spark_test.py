#!/usr/bin/env python
# encoding:utf8

import os
import sys
sys.path.append('.')
import spark_lib
import json
from operator import add

PROD_CORE = '/personal/yuanyuan/base_data/base_products_core_insell.dat'
HDFS_PATH = '/groups/reco/reco_arch/merge_layer/alsobuy/test_a/result_data'
SAVE_HDFS = '/personal/yuanyuan/save_test'


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
            pid, info = line.strip().split('\t')
            recos = json.loads(info)
        except Exception:
            return ()
        if pid != '23816437':
            return None
        return line

    @staticmethod
    def parse_insell(line):
        items = line.strip().split('\t')
        pid = int(items[0])
        cpath = items[1][:2]
        sid = int(items[3])
        '''
        if cpath != '01':
            return ()
        '''
        return None

    @classmethod
    def test(cls):
        '''
        global INSELL_PID
        pids_lst = spark_lib.spark_read_hdfs(cls.sc, PROD_CORE)\
                            .map(cls.parse_insell).filter(None).collectAsMap()
        INSELL_PID = cls.sc.broadcast(pids_lst)
        '''
        new_rdd = spark_lib.spark_read_hdfs(cls.sc, HDFS_PATH)\
                           .map(cls.parse_reco).filter(None)
        print 'target line:', new_rdd.take(1)
        spark_lib.save_rdd_to_hdfs(new_rdd, SAVE_HDFS)
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
