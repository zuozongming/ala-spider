#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
"""
File: gen_crawler_query.py
Author: yudonglee(yudonglee@baidu.com)
Date: 2014/11/11 23:20:15
"""
import logging
import argparse
import ConfigParser
import sys
import os

if __name__ == "__main__":
    #parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", dest = "query_file_path", required = True, help = "���query")
    parser.add_argument("-o", dest = "output_path", required = True, help = "����ļ�·��")
    args = parser.parse_args()

    #logging
    log_path = "../log/"
    log_name = "gen_crawler_query"
    logger = logging.getLogger(log_name)
    #logger.setLevel(logging.DEBUG) 
    logger.setLevel(logging.INFO) 

    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(process)s ' + 
        '[%(filename)s:%(lineno)s] %(message)s')  

    fh = logging.FileHandler(log_path + log_name + ".log")  
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter) 
    logger.addHandler(fh)


    line_counter = 0
    query_file_handler = open(args.query_file_path, "r")

    query_dict = {}
    for line in query_file_handler:
        line_counter += 1
        line = line.rstrip("\r\n ")
        cols = line.split("\t")

        cur_col_idx = 0
        query = cols[cur_col_idx]
        if len(query) == 0:
            continue
        query_dict[query] = 1
    try:
        output_file = open(args.output_path, 'w')
    except IOError as e:
        logger.warning("fail to open file[%s]" % args.output_path) 

    env_dict = {}
    env_dict['online'] = {}
    env_dict['online']['url_param'] = 'sid=5581'

    env_dict['sample'] = {}
    env_dict['sample']['url_param'] = 'sid=1'

    #env_dict['pad_sample'] = {}
    #env_dict['pad_sample']['url_param'] = 'sid=9723&dsp=ipad'

    for query, query_v in query_dict.iteritems():
        if len(query) == 0:
            continue
        sample_str = "%d" % (len(env_dict))
        for sid_k, sid_v in env_dict.iteritems():
            sample_str += "\t%s\t%s" % (sid_k, sid_v['url_param'])
        output_file.write("%s\t%s\n"%(query, sample_str))
