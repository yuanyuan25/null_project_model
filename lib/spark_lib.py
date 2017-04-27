#!/usr/bin/env python
# encoding:utf8
'''
spark 初始化
通过配置文件进行初始化，默认配置文件位置为：
    path:       conf/main.cfg
    section:    [SPARK]

spark配置文件格式：
    [SPARK]
    master = xxx(必须, 值不为空)
    app_name = xxx(必须, 值不为空)
    files = xxx, yyy...(必须，值可为空)
    zzz = xxx

    其他参数命名方式：
        若，spark配置的原名称为：spark.xxx.yyy
        则，[SPARK]中配置项名称：xxx_yyy(区分大小写)
        example:
            原名称：spark.akka.frame.Size
            配置项：akka_frame_Size = 128

by: yuanyuan
'''
import os
import re
import timeutil
import awesome_hdfs
import json
from ConfigParser import SafeConfigParser
from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles


__author__ = 'yuanyuan'

__all__ = ['init_spark', 'release_spark', 'get_file_path',
           'save_rdd_to_hdfs', 'save_rdd_by_cfg', 'spark_read_hdfs']


class Color(object):
    black = "\033[30m"
    red = "\033[31m"
    green = "\033[32m"
    yellow = "\033[33m"
    blue = "\033[34m"
    purple = "\033[35m"
    sky = "\033[36m"
    white = "\033[37m"


def get_spark_sc(main_cfg):
    '''配置spark'''
    default_options = set(main_cfg.defaults())
    spark_options = set(main_cfg.options('SPARK')) - default_options

    spark_cfg = SparkConf()
    for opt in spark_options:
        value = main_cfg.get('SPARK', opt)
        if opt == 'master':
            spark_cfg.setMaster(value)
        elif opt == 'app_name':
            spark_cfg.setAppName(value)
        elif opt == 'files':
            continue
        else:
            key = 'spark.%s' % opt.replace('_', '.')
            spark_cfg.set(key, value)

    spark_sc = SparkContext(conf=spark_cfg)

    return spark_sc


def init_spark(conf='conf/main.cfg'):
    '''初始化spark并上传依赖文件
    conf: spark配置文件路径或句柄,默认值为'conf/main.cfg'
    example:
        init_spark('conf/main.cfg')
      or
        main_cfg = SafeConfigParser()
        main_cfg.read('conf/main.cfg')
        init_spark(main_cfg)
    '''

    if isinstance(conf, str):
        main_cfg = SafeConfigParser()
        main_cfg.read(conf)
    else:
        main_cfg = conf

    spark_sc = get_spark_sc(main_cfg)

    files = main_cfg.get('SPARK', 'files')
    files = files.split(',')
    additional_files = ['/lib/python_lib/awesome_hdfs.py',
                        '/lib/python_lib/timeutil.py',
                        '/lib/python_lib/spark_lib.py']
    files.extend(additional_files)
    for fname in files:
        spark_sc.addFile(fname)
    return spark_sc


def release_spark(spark_sc):
    '''释放sc句柄'''
    spark_sc.stop()
    return


def get_file_path(fname):
    '''获取上传到集群的文件的路径
    fname：不带路径的文件名
    example：
        sc.addfile('static/a.dat')
        path = get_file_path('a.dat')'''
    return SparkFiles.get(fname)


def rdd_to_json(items):
    '''rdd转成json
    output: key\tvalue
    '''
    key, value = items
    if isinstance(value, dict) and 'reco_lst' in value:
        output_str = '%s\t%s' % (key, json.dumps(value))
        return output_str

    value_json = {"reco_lst": []}
    for item in value:
        reco_json = {"reco_id": 0, "weight": 0}
        if isinstance(item, int) or isinstance(item, str):
            reco_json["reco_id"] = item
        elif isinstance(item, dict) and 'reco_id' in item:
            reco_json = item
        elif isinstance(item, tuple) or isinstance(item, list):
            reco_json["reco_id"] = item[0]
            reco_json["weight"] = item[-1]
            if len(item) == 3:
                reco_json['lineage'] = item[1]
        else:
            continue
        value_json["reco_lst"].append(reco_json)

    output_str = '%s\t%s' % (key, json.dumps(value_json))
    return output_str


def rdd_to_str(items):
    '''rdd转成str
    output: key\tvalue
    '''
    output_str = '%s\t%s' % (items[0], items[1])
    return output_str


def save_rdd_by_cfg(rdd, cfg, hdfs_section, hdfs_key, local_section='',
                    local_key='', is_save=True, outformat='str'):
    '''保存rdd到hdfs,路径读取配置文件
    cfg: 配置文件对象
    hdfs_section： hdfs路径所在section
    hdfs_key：路径对应的配置文件key值
    local_section: 本地路径所在section(若不存本地则为空)
    local_key: 存到本地的路径对应的配置文件的key值(若不存本地则为空)
    '''
    if not is_save:
        return
    hdfs_path = cfg.get(hdfs_section, hdfs_key)
    if local_section and local_key:
        local_path = cfg.get(local_section, local_key)
    else:
        local_path = ''

    save_rdd_to_hdfs(rdd, hdfs_path, local_path, is_save, outformat)
    return


def save_rdd_to_hdfs(rdd, hdfs_path, local_path='',
                     is_save=True, outformat='str'):
    '''保存rdd到hdfs
    hdfs_path: hdfs保存路径
    local_path: 存到本地的路径(若不存本地则为空)
    format: 默认str，可选【json】
    若为json，则一般为推荐数据，此时需要保证数据格式为：
        （main_pid, [(reco_pid,weight),(reco_pid,weight),...]）
    '''
    if not is_save:
        return
    if not awesome_hdfs.exist(hdfs_path):
        awesome_hdfs.mkdir(hdfs_path)
    awesome_hdfs.rm(hdfs_path)

    if outformat == 'str':
        format_func = rdd_to_str
    elif outformat == 'json':
        format_func = rdd_to_json
    rdd.map(format_func).coalesce(120).saveAsTextFile(hdfs_path)

    if not local_path:
        return
    try:
        awesome_hdfs.getmerge(hdfs_path, local_path)
    except Exception, error:
        print 'Result getmerge to local error: %s' % error
    return


def parse_path_lst(path_lst):
    '''解析输入路径，由{date:from_day:end_day:-}转换成压缩的可供spark解析的日期形式
    '''
    patterns = []
    input_lst = []
    date_format = timeutil.get_date_fmt()
    re_str = r'({\s*date\s*:\s*([+-]?\d+)+\s*:\s*([+-]?\d+)+\s*:\s*([^\s}])+\s*})'
    for path in path_lst:
        matches = re.findall(re_str, path, re.I | re.U)
        if matches:
            stub, from_day, end_day, sep = matches[0]
            from_date = timeutil.fore_n_days(int(from_day), date_format)
            end_date = timeutil.fore_n_days(int(end_day), date_format)
            min_date, max_date = min(from_date, end_date), max(from_date, end_date)
            date_lst = timeutil.short_span_dates(min_date, max_date, sep)
            for date in date_lst:
                input_path = path.replace(stub, date)
                input_lst.append(input_path)
        else:
            input_lst.append(path)

    return input_lst


def check_input_path(input_lst, check_existance):
    '''检测输入目录是否存在，如果不存在，则删除
    '''
    valid_input_path = []
    total = len(input_lst)
    print '%sChecking path num:%s %s' % (Color.green, total, Color.white)
    if check_existance:
        for idx, input in enumerate(input_lst):
            if not awesome_hdfs.exist(input):
                print '%s[Warning    ][Process %s/%s] %s Not Found%s' %\
                    (Color.red, idx+1, total, input, Color.white)
            else:
                valid_input_path.append(input)
                print '%s[Checking   ][Process %s/%s] %s Exist%s' %\
                    (Color.green, idx+1, total, input, Color.white)

    return valid_input_path


def union_input_data(spark_sc, input_path_lst):
    '''按不同的日期的输入目录读数据存到rdd并union起来
    '''
    rdd_lst = []
    for input_path in input_path_lst:
        rdd = spark_sc.textFile(input_path)
        rdd_lst.append(rdd)
    return spark_sc.union(rdd_lst)


def spark_read_hdfs(spark_sc, path, check_existance=True):
    '''读取hdfs文件，主要是对输入进行处理，适配list,日期{date:from_day:end_day:-}
    path:"/xxx/xxx,/xxx/yyy" or ["/xxx/xxx", "/xxx/yyy",...]
    for example:
        path = '/share/comm/ddclick/{date:-1:-10:-}/ddclick_product'
        表示输入日期是从昨天一直到十天前的数据
    '''
    if isinstance(path, str):
        path_lst = path.strip().replace(' ', '').strip("\"").split(',')
    else:
        path_lst = path

    input_lst = parse_path_lst(path_lst)
    print '%s[Input Path ]%s%s' % (Color.green, input_lst, Color.white)
    valid_input_path = check_input_path(input_lst, check_existance)
    if not valid_input_path:
        raise Exception('Input path is NULL!!!')
    unioned_rdd = union_input_data(spark_sc, valid_input_path)
    return unioned_rdd


if __name__ == '__main__':
    SPARK_SC = init_spark()
    release_spark(SPARK_SC)
