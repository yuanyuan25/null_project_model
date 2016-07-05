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
    data_dir = '/d3/${USER}/%s' % proj_name
    hdfs_home = '/personal/${USER}/%s' % proj_name
    cmd = 'sed -i "s#%s.*=.*#%s=%s#g" %s'
    print 'init log.cfg'
    os.system(cmd % ('proj_name', 'proj_name', proj_name, 'conf/log.cfg'))
    print 'init main.cfg'
    os.system(cmd % ('proj_name', 'proj_name', proj_name, 'conf/main.cfg'))
    os.system(cmd % ('hdfs_dir', 'hdfs_dir', hdfs_home, 'conf/main.cfg'))
    print 'init deploy.sh'
    os.system(cmd % ('DATA_DIR', 'DATA_DIR', data_dir, 'deploy.sh'))
    os.system(cmd % ('HDFS_HOME', 'HDFS_HOME', hdfs_home, 'deploy.sh'))
    print 'init main.py'
    os.system('mv main.py %s_main.py' % proj_name)
    print 'init page_show'
    os.system('mv show_result/show.py show_result/show_%s.py' % proj_name)
    os.system('mv show_result/show.html show_result/show_%s.html' % proj_name)
    print 'init run.sh'
    os.system('sed -i "s#main#%s_main#g" %s' % (proj_name, 'run.sh'))
    print 'end!'
    return


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'must run with arg like: init_project.py project_name'
    elif sys.argv[1] == 'recover':
        main('null_project')
    else:
        main(sys.argv[1])
