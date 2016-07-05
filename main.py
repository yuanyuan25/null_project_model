#!/usr/bin/env python
# encoding:utf8
'''
info:
'''

import sys
sys.path.append('./modules')
from test_spark import A
from spark_lib import init_spark, release_spark
from send_email import send_html_mail
from safe_mad_call import safe_mad_call
from common.global_vars import LOGGER, MAIN_CFG
from ConfigParser import SafeConfigParser

PARAM_SECTION = 'param'
CONF_FILE_SECTION = 'conf_file'

RUN_JOBS = 'run_jobs.py -C'
JOB_NAMES = []

SPARK_SC = init_spark(MAIN_CFG)


def run_mr_job(job_name):
    '''运行mr作业'''
    LOGGER.info('Running job: %s...', job_name)
    conf_file = MAIN_CFG.get(CONF_FILE_SECTION, job_name)
    cmd = '%s %s' % (RUN_JOBS, conf_file)
    safe_mad_call(cmd)
    LOGGER.info('End job: %s...', job_name)
    return


def prepare_data():
    '''准备数据，数据量较大，需要用mr作业进行预处理，生成输入数据'''
    # 修改需要解析的行为数据的时间段
    from_day = MAIN_CFG.get(PARAM_SECTION, 'from_day')
    end_day = MAIN_CFG.get(PARAM_SECTION, 'end_day')
    conf_file = MAIN_CFG.get(CONF_FILE_SECTION, 'job_name')
    job_cfg = SafeConfigParser()
    job_cfg.read(conf_file)
    job_cfg.set('DEFAULT', 'from_day', from_day)
    job_cfg.set('DEFAULT', 'end_day', end_day)
    with open(conf_file, 'w') as fp_file:
        job_cfg.write(fp_file)

    # 运行行为数据解析作业
    for name in JOB_NAMES:
        run_mr_job(name)
    return


def main():
    '''函数主入口'''
    LOGGER.info('Prepare data, run jobs...')
    # prepare_data()

    LOGGER.info('Init spark')
    A.init(MAIN_CFG, SPARK_SC)

    LOGGER.info('test...')
    A.test()

    release_spark(SPARK_SC)
    return


if __name__ == '__main__':
    APP_NAME = MAIN_CFG.get('mail', 'app_name')
    MAIL_FROM = MAIN_CFG.get("mail", "mail_from")
    MAIL_TO = MAIN_CFG.get("mail", "mail_to").split(",")
    MAIL_TITLE = MAIN_CFG.get("mail", "mail_title")
    try:
        main()
        SUCCESS_INFO = 'Application %s succeed!' % APP_NAME
        MAIL_TITLE = '%s success!!!' % MAIL_TITLE
        send_html_mail(MAIL_FROM, MAIL_TO, MAIL_TITLE, SUCCESS_INFO)
    except Exception, error:
        LOGGER.exception("The %s has error,detail:\n %s", APP_NAME, error)
        MAIL_TITLE = '%s error!!!' % MAIL_TITLE
        send_html_mail(MAIL_FROM, MAIL_TO, MAIL_TITLE, str(error)[:2000])
        exit(1)
