#!/usr/bin/env python
# encoding:utf8
'''
公共全局变量
'''
import logging.config
from ConfigParser import SafeConfigParser

MAIN_CFG = SafeConfigParser()
MAIN_CFG.optionxform = str
MAIN_CFG.read('conf/main.cfg')

logging.config.fileConfig('conf/log.cfg')
LOGGER = logging.getLogger('reco')
