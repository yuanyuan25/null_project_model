#!/usr/bin/env python
# encoding:utf8


import os
import sys
sys.path.append('.')
sys.path.append('./modules')
from common.global_vars import LOGGER, MAIN_CFG
# from common.spark_init import init_spark
from common.spark_init import SPARK_SC
import test_spark

# SPARK_SC = init_spark(MAIN_CFG)


def main():
    LOGGER.info('test!')
    # os.system('spark-submit modules/test_spark.py')
    test_spark.test(SPARK_SC)
    # test_spark.main()

    return


if __name__ == '__main__':
    main()
