#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
"""
File: save_crawler_result.py
Author: yudonglee(yudonglee@baidu.com)
Date: 2014/11/12 16:54:40
"""
import logging
import argparse
import ConfigParser
import sys
import os

import monitor_content_parser

if __name__ == "__main__":
    #parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", dest = "crawler_result_path", required = True, help = "query抓取结果")
    parser.add_argument("-s", dest = "save_dict_path", required = True, help = "内容保存路径")
    args = parser.parse_args()

    #logging
    log_path = os.path.dirname(os.path.realpath(__file__))
    log_path += "/../log/"
    log_name = "save_crawler_result"
    logger = logging.getLogger(log_name)
    #logger.setLevel(logging.DEBUG) 
    logger.setLevel(logging.INFO) 

    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(process)s ' + 
        '[%(filename)s:%(lineno)s] %(message)s')  

    fh = logging.FileHandler(log_path + log_name + ".log")  
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter) 
    logger.addHandler(fh)

    content_dict = {}
    
    monitor_parser = monitor_content_parser.MonitorContentParser(logger)

    ret = monitor_parser.load_content_dict(args.save_dict_path, content_dict)
    if ret != 0:
        sys.exit(1)

    ret = monitor_parser.add_content_dict(args.crawler_result_path, content_dict)
    if ret != 0:
        sys.exit(1)

    ret = monitor_parser.save_content_dict(args.save_dict_path, content_dict)
    if ret != 0:
        sys.exit(1)
