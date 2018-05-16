#!/usr/bin/env python
# encoding:utf8

import os
import sys
sys.path.append('.')
import spark_lib
import json
import urlparse
import awesome_hdfs
import random
from util import URLUtil
from operator import add
from pyspark.mllib.recommendation import Rating

PERM_PATH = '/groups/ddclick/perm_cust/result'
PROD_CORE = '/groups/reco/reco_job_base_data/put_files_position/base_products_core_insell.dat'
PROD_CATE = '/groups/reco/reco_job_base_data/put_files_position/base_category.dat'
CLICK_DAILY_PATH = '/groups/reco/click_daily_pull/{date:-1:-7:-}'
MERGE_PATH = '/groups/reco/reco_arch/merge_layer/prefer_pids/test_a/result_data'
# FILTER_PATH = '/groups/reco/reco_arch/filter_layer/alsobuy/test_a/result_data'
FILTER_PATH = '/personal/yuanyuan/reco_arch/filter_layer/alsobuy/test_a/result_data'
# HDFS_PATH = '/personal/yuanyuan/update_user_label_to_es/result_data'
PC_CLICK_PATH = '/share/comm/ddclick/{date:-1:-90:-}/ddclick_product/'
APP_CLICK_PATH = '/share/comm/kafka/client/{date:-1:-90:-}/*'
H5_CLICK_PATH = '/share/comm/kafka/wap/{date:-1:-90:-}/*'
SAVE_HDFS = '/personal/yuanyuan/test/perm'
INSELL_PID_PATH = '/personal/yuanyuan/test/insell_pids'
ALS_PATH = '/groups/reco/reco_arch/reco_unit_layer/user_matrix_factorization_data/User_reco_pid_pv_%s'
ALS_EXT_PATH = '/groups/reco/reco_arch/reco_unit_layer/user_matrix_factorization_data/User_reco_cate_pids'
FEEDID2PID_PATH = '/personal/yuanyuan/feed_reco/feedid2pid.dat'
FEEDINFO_PATH = '/personal/yuanyuan/feed_reco/feed_info.dat'
FEED_HDFS_PATH = '/personal/yuanyuan/feed_reco/feed_info_merge'
FEED_LOCAL_PATH = '/d1/home/yuanyuan/base_data/feed_info_merge.dat'
OFFLINE_KPI_DATA = ''
RECOSYS_LOG = '/groups/reco/recosys_log/{date:-1:-30:-}'


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
    def parse_reco_and_seach(line):
        try:
            pid, reco_info = line.strip().split('\t')
            reco = json.loads(reco_info)
        except Exception:
            return ()
        if pid != '1280268076':
            return None
        return line

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
    def test_perm_num(cls):
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
                            .coalesce(1200).reduceByKey(add).count()

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

    @staticmethod
    def extract_perm(line):
        perm = line.strip().split('\t')[0]
        return (len(perm), 1)

    @classmethod
    def test_als(cls):
        num = spark_lib.read_hdfs(cls.sc, HDFS_PATH).map(cls.extract_perm).reduceByKey(add).collectAsMap()
        print num
        num = spark_lib.read_hdfs(cls.sc, ALS_EXT_PATH).map(cls.extract_perm).reduceByKey(add).collectAsMap()
        print num
        num = spark_lib.read_hdfs(cls.sc, ALS_PATH % 'babycloth').map(cls.extract_perm).reduceByKey(add).collectAsMap()
        print num
        num = spark_lib.read_hdfs(cls.sc, ALS_PATH % 'mancloth').map(cls.extract_perm).reduceByKey(add).collectAsMap()
        print num
        num = spark_lib.read_hdfs(cls.sc, ALS_PATH % 'motherbaby').map(cls.extract_perm).reduceByKey(add).collectAsMap()
        print num
        num = spark_lib.read_hdfs(cls.sc, ALS_PATH % 'womancloth').map(cls.extract_perm).reduceByKey(add).collectAsMap()
        print num
        num = spark_lib.read_hdfs(cls.sc, ALS_PATH % 'outside').map(cls.extract_perm).reduceByKey(add).collectAsMap()
        print num
        return

    @staticmethod
    def extract_pid(line):
        tokens = line.strip().split('\t')
        pid = int(tokens[0])
        sale = int(tokens[3])
        cpath = tokens[7]
        if not cpath.startswith('01'):
            return ()
        return (pid, sale)


    @staticmethod
    def filter_pids(item):
        pid = item[0]
        sale = item[1]
        if sale < 10:
            return False
        return True

    @staticmethod
    def extract_click_pid(line):
        tokens = line.strip().split('\t')
        try:
            pid = int(tokens[2])
        except Exception:
            return ()
        return (pid, 1)

    @staticmethod
    def extract_click_uid(line):
        tokens = line.strip().split('\t')
        try:
            uid = int(tokens[1])
        except Exception:
            return ()
        if not uid:
            return ()
        return (uid, 1)

    @classmethod
    def extract_insell_pid(cls):
        rdd = spark_lib.read_hdfs(cls.sc, PROD_CORE)\
                .map(lambda line: line.strip().split('\t')[0])

        spark_lib.save2hdfs(rdd, INSELL_PID_PATH, outformat="origin")
        awesome_hdfs.getmerge(INSELL_PID_PATH, '~/base_data/insell_pids.dat')
        return

    @classmethod
    def extract_hot_pid(cls):
        rdd = spark_lib.read_hdfs(cls.sc, CLICK_DAILY_PATH)\
            .map(cls.extract_click_pid).filter(None).reduceByKey(add)\
            .filter(cls.filter_pids).collectAsMap()
        print 'length:', len(rdd)
        pid_list = sorted(rdd.iteritems(), key=lambda x: x[1], reverse=True)[:30000]
        print 'length:', len(pid_list)
        with open('/d1/home/yuanyuan/base_data/hot_pids.dat', 'w') as fp_hot:
            for pid in pid_list:
                print >> fp_hot, pid[0]

        # spark_lib.save2hdfs(rdd, INSELL_PID_PATH)
        # awesome_hdfs.getmerge(INSELL_PID_PATH, '~/base_data/insell_pids.dat')
        return pid_list

    @staticmethod
    def extract_feedid2pid(line):
        tokens = line.strip().split('\t')
        if len(tokens)<2:
            return ()
        fid = tokens[0]
        pid = int(tokens[1])
        return (fid, [pid])

    @staticmethod
    def extract_feed_info(line):
        tokens = line.strip().split('\t')
        if not tokens:
            return ()
        feed_id = tokens[0]
        ori_id = tokens[1]
        if not ori_id:
            return ()
        title = tokens[2]
        img_url = tokens[3]
        feed_type = tokens[4]
        feed_name = tokens[5]

        return (ori_id, [feed_id, title, img_url, feed_type, feed_name])

    @staticmethod
    def transfid2pid(item):
        fid2pid_dict = FID2PID_MAP.value
        fid, value = item
        pid = fid2pid_dict.get(fid, [])
        if not pid:
            return ()
        pid = random.choice(pid)
        return (pid, str(value))


    @classmethod
    def merge_feed_data(cls):
        feedid2pid_dict = spark_lib.read_hdfs(cls.sc, FEEDID2PID_PATH)\
            .map(cls.extract_feedid2pid).filter(None)\
            .reduceByKey(add).collectAsMap()
        print "length:", len(feedid2pid_dict)
        global FID2PID_MAP
        FID2PID_MAP = cls.sc.broadcast(feedid2pid_dict)
        feed_info_rdd = spark_lib.read_hdfs(cls.sc, FEEDINFO_PATH)\
            .map(cls.extract_feed_info).filter(None)\
            .map(cls.transfid2pid).filter(None)

        spark_lib.save2hdfs(feed_info_rdd, FEED_HDFS_PATH)
        awesome_hdfs.getmerge(FEED_HDFS_PATH, FEED_LOCAL_PATH)
        return

    @classmethod
    def get_active_custid(cls):
        uid_dict = spark_lib.read_hdfs(cls.sc, CLICK_DAILY_PATH)\
            .map(cls.extract_click_uid).filter(None)\
            .reduceByKey(add).filter(lambda x: x[1]>5).collectAsMap()
        pid_list = sorted(uid_dict.iteritems(), key=lambda x: x[1], reverse=True)
        print pid_list[1]
        if len(pid_list) > 10000:
            pid_list = pid_list[:10000]
        print 'length:', len(pid_list)
        with open('/d1/home/yuanyuan/base_data/active_uids.dat', 'w') as fp_active:
            for pid in pid_list:
                print >> fp_active, pid[0]

        return

    @staticmethod
    def parse_json(line):
        tokens = line.strip().split('\t')
        a = json.loads(tokens[0])
        return a

    @classmethod
    def test_json(cls):
        hdfs_path = '/groups/reco/readdp/category_info.json'
        rdd = spark_lib.read_hdfs(cls.sc, hdfs_path)\
            .map(cls.parse_json)
        print rdd.take(1)
        return

    @staticmethod
    def parse_log(line):
        tokens = line.strip().split(' ')
        if 'IN-JSON' not in line:
            return ()
        time = tokens[1]
        if time < '04:00:00' or time > '12:00:00':
            return ()

        return (time, 1)

    @classmethod
    def test_tps(cls):
        hdfs_path = '/groups/reco/recosys_log/2018-04-20'
        data = spark_lib.read_hdfs(cls.sc, hdfs_path)\
            .map(cls.parse_log).filter(None).reduceByKey(add).collectAsMap()
        print len(data)
        lst = sorted(data.iteritems(), key=lambda x:x[1])
        print lst[-50:]
        return

    @classmethod
    def search_pid(cls):
        data = spark_lib.read_hdfs(cls.sc, FILTER_PATH)\
            .map(cls.parse_reco_and_seach).filter(None).collect()
        print data
        return

    @classmethod
    def check_kpi(cls):
        data = spark_lib.read_hdfs(cls.sc, OFFLINE_KPI_DATA)\
            .map(cls.parse_kpi_data).filter(None).collect()
        return

    @staticmethod
    def parse_recosys_log(line):
        if 'OUT-JSON' not in line:
            return ()
        tokens = line.strip().split('OUT-JSON')
        if len(tokens) != 2:
            return ()
        head, out_info = tokens
        head_lst = head.split(' ')
        date = head_lst[0][1:].replace('/', '')[4:]
        time = head_lst[1].replace(':', '')[:-2]
        try:
            out = json.loads(out_info.strip())
        except:
            return ()

        if out['isTest']:
            return ()

        key = '%s%s_%s' % (date, time, out['method'])
        value = (out['total_count'], 1)

        return (key, value)

    @classmethod
    def check_reco_log_out(cls):
        data = spark_lib.read_hdfs(cls.sc, RECOSYS_LOG)\
            .map(cls.parse_recosys_log).filter(None)\
            .reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))
        time_module = data.collectAsMap()
        time_all = data.map(lambda x:(x[0].split('_')[0], x[1]))\
            .reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))\
            .collectAsMap()

        time_module_lst = sorted(time_module.iteritems(), key=lambda x:x[0])
        time_all_lst = sorted(time_all.iteritems(), key=lambda x:x[0])
        with open('time_module.dat', 'w') as fp_m:
            for item in time_module_lst:
                key, (sum_out, count) = item
                print>> fp_m, '%s,%d,%s,%s' % (key, sum_out*1.0/count, sum_out, count)
        with open('time_all.dat', 'w') as fp_m:
            for item in time_all_lst:
                key, (sum_out, count) = item
                print>> fp_m, '%s,%d,%s,%s' % (key, sum_out*1.0/count, sum_out, count)

        return

    @classmethod
    def test(cls):
        # cls.test_als()
        # cls.extract_insell_pid()
        # cls.extract_hot_pid()
        # cls.merge_feed_data()
        # cls.get_active_custid()
        # cls.test_json()
        # cls.test_tps()
        # cls.search_pid()
        # cls.check_kpi()
        cls.check_reco_log_out()
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
