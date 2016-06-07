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
        spark配置的原名称：spark.xxx.yyy
        [SPARK]中配置项名称：xxx_yyy(区分大小写)
        example:
            spark.akka.frame.Size
            -> akka_frame_Size = 128
'''
from pyspark import SparkFiles
from ConfigParser import SafeConfigParser


def get_spark_sc(spark_context, spark_conf, main_cfg):
    '''配置spark'''
    default_options = set(main_cfg.defaults())
    spark_options = set(main_cfg.options('SPARK')) - default_options

    for opt in spark_options:
        value = main_cfg.get('SPARK', opt)
        if opt == 'master':
            spark_conf().setMaster(value)
        elif opt == 'app_name':
            spark_conf().setAppName(value)
        elif opt == 'files':
            continue
        else:
            key = 'spark.%s' % opt.replace('_', '.')
            spark_conf().set(key, value)

    spark_sc = spark_context(conf=spark_conf())

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
    from pyspark import SparkContext, SparkConf

    if isinstance(conf, str):
        main_cfg = SafeConfigParser()
        main_cfg.read(conf)
    else:
        main_cfg = conf

    spark_sc = get_spark_sc(SparkContext, SparkConf, main_cfg)

    files = main_cfg.get('SPARK', 'files')
    files = files.split(',')
    for fname in files:
        print fname
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

# SPARK_SC = init_spark()
if __name__ == '__main__':
    SPARK_SC = init_spark()
    release_spark(SPARK_SC)
