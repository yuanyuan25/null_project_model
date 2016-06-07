#!/usr/bin/env python
# encoding:utf8
'''
将空项目模型按项目名称初始化为特定的工程项目
主要修改：
    配置文件名称，hdfs工作目录，本地工作目录，log文件生成名称
'''

import os
import sys


def main(proj_name):
    '''初始化工程为特定名称工程'''
    hdfs_home = '/personal/yuanyuan/%s' % proj_name
    cmd = 'sed -i "s#%s.*=.*#%s=%s#g" %s'
    os.system(cmd % ('proj_name', 'proj_name', proj_name, 'conf/log.cfg'))
    os.system(cmd % ('app_name', 'app_name', proj_name, 'conf/main.cfg'))
    os.system(cmd % ('HDFS_HOME', 'HDFS_HOME', hdfs_home, 'deploy.sh'))
    # os.system('mv ../null_project_model ../%s' % proj_name)
    return


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'must run with arg like: init_project.py project_name'
    elif sys.argv[1] == 'recover':
        main('null_project')
    else:
        main(sys.argv[1])
