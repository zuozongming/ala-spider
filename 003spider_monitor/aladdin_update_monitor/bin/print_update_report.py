#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
"""
File: print_update_report.py
Author: yudonglee(yudonglee@baidu.com)
Date: 2014/11/12 16:54:40
"""
import logging
import argparse
import ConfigParser
import sys
import os
import datetime
import urllib

import monitor_content_parser
def load_url_map(url_file_path, url_map):
    try:
        fsock = open(url_file_path, "r", 0)
    except IOError:
        return -1
    line_counter = 0
    for line in fsock:
        line_counter += 1
        line = line.rstrip("\r\n ")
        cols = line.split("\t")
        url = cols[0]
        search_total = int(cols[1])
        click_total = int(cols[2])

        part_idx = url.find("?")
        if part_idx > 0:
            url = url[0:part_idx]

        url_map[url] = {}
        url_map[url]['search_total'] = search_total
        url_map[url]['click_total'] = click_total

def diff_content_time(query, old_list, new_list, max_num=None, encoding="GB18030"):
    diff_result_list = []
    if old_list is None or len(old_list) == 0 or new_list is None or len(new_list) == 0:
        return diff_result_list

    #先去重
    old_content_list = []
    old_content_dedup_map = {}
    for idx, item in enumerate(old_list):
        if len(item['data']) == 0:
            continue
        item_data = item['data']
        #if (len(old_content_list) == 0 or 
        #        item_data['content_key'] != old_content_list[-1]['data']['content_key']):
        if item_data['content_key'] not in old_content_dedup_map:
            old_content_dedup_map[item_data['content_key']] = 1
            old_content_list.append(item)

    if max_num is not None and len(old_content_list) > max_num:
        tmp_start = len(old_content_list) - max_num
        old_content_list = old_content_list[tmp_start:]

    new_content_list = []
    new_content_dedup_map = {}
    for idx, item in enumerate(new_list):
        if len(item['data']) == 0:
            continue
        item_data = item['data']
        #if (len(new_content_list) == 0 or 
        #        item_data['content_key'] != new_content_list[-1]['data']['content_key']):
        if item_data['content_key'] not in new_content_dedup_map:
            new_content_dedup_map[item_data['content_key']] = 1
            new_content_list.append(item)

    if max_num is not None and len(new_content_list) > max_num:
        tmp_start = len(new_content_list) - max_num
        new_content_list = new_content_list[tmp_start:]

    

    if len(old_content_list) == 0 or len(new_content_list) == 0:
        return diff_result_list
    
    result_type = new_content_list[-1]['data']['type']


    old_list_idx = 0
    new_list_idx = 0
    date_format_str = '%Y-%m-%d'
    time_format_str = '%Y-%m-%d %H:%M:%S'
    while(True):
        if new_list_idx >= len(new_content_list) or old_list_idx >= len(old_content_list):
            break
        new_item = new_content_list[new_list_idx]
        old_item = old_content_list[old_list_idx]
        new_item_data = new_item['data']
        old_item_data = old_item['data']
        
        if result_type == "zongyi":
            new_latest_episode = new_item_data['latest_episode']
            new_update_time_str = new_item_data['update_time']

            old_latest_episode = old_item_data['latest_episode']
            old_update_time_str = old_item_data['update_time']

            if len(new_latest_episode) == 0:
                new_list_idx += 1
                continue
            if len(old_latest_episode) == 0:
                old_list_idx += 1
                continue
            new_latest_episode = new_latest_episode.rstrip("期".decode(encoding))
            old_latest_episode = old_latest_episode.rstrip("期".decode(encoding))
            try:
                new_content_time = datetime.datetime.strptime(new_latest_episode, date_format_str)
            except ValueError as e:
                new_list_idx += 1
                continue
            try:
                old_content_time = datetime.datetime.strptime(old_latest_episode, date_format_str)
            except ValueError as e:
                old_list_idx += 1
                continue
            tmp_query = query.encode(encoding)
            if new_content_time > old_content_time:
                old_list_idx += 1
                #if query.encode(encoding) == "丑闻第四季":
                #    print "%s,%s,%s:new:" % (tmp_query, new_latest_episode.encode(encoding),
                #        old_latest_episode.encode(encoding))
            elif new_content_time < old_content_time:
                new_list_idx += 1
                #if query.encode(encoding) == "丑闻第四季":
                #    print "%s,%s,%s:old:" % (tmp_query, old_latest_episode.encode(encoding),
                #        new_latest_episode.encode(encoding))
            else:
                old_update_time = datetime.datetime.strptime(old_update_time_str, time_format_str)
                new_update_time = datetime.datetime.strptime(new_update_time_str, time_format_str)

                diff_minutes = 0
                if new_update_time > old_update_time:
                    time_diff = new_update_time - old_update_time
                    diff_minutes = 0 - time_diff.total_seconds()
                elif new_update_time < old_update_time:
                    time_diff = old_update_time - new_update_time
                    diff_minutes = time_diff.total_seconds()

                diff_minutes /= 60

                diff_item = {}
                diff_item['content_key'] = old_latest_episode
                diff_item['type'] = result_type
                diff_item['diff_minutes'] = int(diff_minutes)
                diff_item['update_time_old'] = old_update_time_str
                diff_item['update_time_new'] = new_update_time_str
                #if query.encode(encoding) == "丑闻第四季":
                #    print "%s,%s=%s,%s|" % (tmp_query, old_latest_episode.encode(encoding),
                #        old_update_time_str.encode(encoding), new_update_time_str.encode(encoding))
                old_list_idx += 1
                new_list_idx += 1
                diff_result_list.append(diff_item)
        elif result_type == "comic" or result_type == "tv":
            new_latest_episode = new_item_data['latest_episode']
            new_update_time_str = new_item_data['update_time']

            old_latest_episode = old_item_data['latest_episode']
            old_update_time_str = old_item_data['update_time']

            if len(new_latest_episode) == 0:
                new_list_idx += 1
                continue
            if len(old_latest_episode) == 0:
                old_list_idx += 1
                continue

            if new_latest_episode.isdigit() is False:
                new_list_idx += 1
                continue
            if old_latest_episode.isdigit() is False:
                old_list_idx += 1
                continue
            tmp_query = query.encode(encoding)
            if new_latest_episode > old_latest_episode:
                old_list_idx += 1
            elif new_latest_episode < old_latest_episode:
                new_list_idx += 1
            else:
                old_update_time = datetime.datetime.strptime(old_update_time_str, time_format_str)
                new_update_time = datetime.datetime.strptime(new_update_time_str, time_format_str)

                diff_minutes = 0
                if new_update_time > old_update_time:
                    time_diff = new_update_time - old_update_time
                    diff_minutes = 0 - time_diff.total_seconds()
                elif new_update_time < old_update_time:
                    time_diff = old_update_time - new_update_time
                    diff_minutes = time_diff.total_seconds()

                diff_minutes /= 60

                diff_item = {}
                diff_item['content_key'] = old_latest_episode
                diff_item['type'] = result_type
                diff_item['diff_minutes'] = int(diff_minutes)
                diff_item['update_time_old'] = old_update_time_str
                diff_item['update_time_new'] = new_update_time_str
                #print "%s,%s=%s,%s|" % (tmp_query, old_latest_episode.encode(encoding),
                #        old_update_time_str.encode(encoding), new_update_time_str.encode(encoding))
                old_list_idx += 1
                new_list_idx += 1
                diff_result_list.append(diff_item)

            #print new_latest_episode,new_update_time, old_latest_episode, old_update_time
            pass
        else:
            old_list_idx += 1
            new_list_idx += 1

    return diff_result_list

def print_udpate_report(output_path, key_set, monitor_dict, url_dict, data_config, encoding="GB18030"):
    try:
        fsock = open(output_path, "w", 0)
    except IOError:
        return -1

    delete_query = {}
    counter = 0
    dedup_url_map = {}
    report_dict = {}
    report_dict['total_time'] = 0
    report_dict['fast_time'] = 0
    report_dict['slow_time'] = 0
    report_dict['total_url'] = 0
    report_dict['fast_url_num'] = 0
    report_dict['same_url_num'] = 0
    report_dict['slow_url_num'] = 0

    report_dict['total_pv'] = 0
    report_dict['fast_pv_num'] = 0
    report_dict['same_pv_num'] = 0
    report_dict['slow_pv_num'] = 0
    for query, item in monitor_dict.iteritems(): 
        counter += 1
        if query.encode(encoding) in delete_query:
            continue
        for item_k, item_list in item.iteritems():
            if item_k not in key_set or item_k == "online":
                continue
            item_v = item_list[-1]
            item_data = item_v['data']
            key = item_k.encode(encoding)
            sample_id = item_v['sample_id']
            srcid = 0
            url = ""
            title = ""
            position = -1
            content_key = ""
            update_time = ""
            content = ""
            total_update_freq = 0
            total_delay_freq = 0
            average_time = 0
            item_sum = len(item_list)

            if len(item_data) > 0:
                srcid = int(item_data['srcid'])
                url = item_data['url'].encode(encoding)
                title = item_data['title'].encode(encoding)
                position = int(item_data['position'])
                content_key = item_data['content_key'].encode(encoding)
                update_time = item_data['update_time'].encode(encoding)
                content_type = item_data['type'] 
                if content_type == "movie":
                    content += "电影：%s" % (content_key)
                elif content_type == "tv":
                    content += "电视剧：%s集" % (content_key)
                elif content_type == "comic":
                    content += "动漫：%s集" % (content_key)
                elif content_type == "zongyi":
                    if content_key.find("期") > 0:
                        content += "综艺：%s" % (content_key)
                    else:
                        content += "综艺：%s期" % (content_key)
                else:
                    content += "其它：%s" % (content_key)

            status = "正常"
            is_ok = True
            if key == "online" and len(item_data) == 0:
                status = "影视未展现"
                is_ok = False
            elif key != "online" and srcid != data_config[key]['target_srcid']:
                status = "%d未展现" % (data_config[key]['target_srcid'])
                is_ok = False
            elif key != "online" and 'diff_list' in item_v and len(item_v['diff_list']) > 0:
                status = item_v['diff_list'][0][1]
                status = status.encode(encoding)

                diff_content_list = diff_content_time(query, item_list, item['online'])
                if (len(diff_content_list) > 0):
                    if (diff_content_list[-1]['diff_minutes'] < 0):
                        status = "更新正常(快%d分)" % (0 - diff_content_list[-1]['diff_minutes'])
                    elif (diff_content_list[-1]['diff_minutes'] > 0):
                        status = "更新延时(慢%d分)" % (diff_content_list[-1]['diff_minutes'])
                total_update_freq = len(diff_content_list)
                for tmp_diff_item in diff_content_list:
                    average_time += tmp_diff_item['diff_minutes']
                    if tmp_diff_item['diff_minutes'] > 0:
                        total_delay_freq += 1
                if total_update_freq != 0:
                    average_time /= total_update_freq
                average_time = int(average_time)
                is_ok = False
            average_time_str = "0"
            if average_time > 0:
                average_time_str = 0 - average_time
            elif average_time < 0:
                average_time_str = 0 - average_time
            video_url = ""

            for tmp_step in range(0, len(item['online'])):
                tmp_idx = (-1 - tmp_step)
                if 'url' in item['online'][tmp_idx]['data']:
                    video_url = item['online'][tmp_idx]['data']['url']
                    break
            if total_update_freq < 2:
                continue
            if len(video_url) == 0:
                continue
            if video_url in dedup_url_map:
                continue
        
            video_url = video_url.encode(encoding)
            part_idx = video_url.find("?")
            if part_idx > 0:
                video_url = video_url[0:part_idx]


            if video_url not in url_dict:
                continue
            search_total = url_dict[video_url]['search_total']
            click_total = url_dict[video_url]['click_total']

            average_time_str = int(average_time_str)

            report_dict['total_time'] += average_time_str
            report_dict['total_url'] += 1
            report_dict['total_pv'] += search_total
            if average_time_str > 0:
                report_dict['fast_url_num'] += 1
                report_dict['fast_pv_num'] += search_total
                report_dict['fast_time'] += average_time_str
            elif average_time_str < 0:
                report_dict['slow_url_num'] += 1
                report_dict['slow_pv_num'] += search_total
                report_dict['slow_time'] += average_time_str
            else:
                report_dict['same_url_num'] += 1
                report_dict['same_pv_num'] += search_total

            dedup_url_map[video_url] = 1
            fsock.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (query.encode(encoding), 
                    video_url,
                    search_total,
                    click_total,
                    total_update_freq, total_delay_freq, 
                    average_time_str, content, 
                    update_time,
                    status))
    print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (report_dict['total_time'],
            report_dict['fast_time'],
            report_dict['slow_time'],
            report_dict['total_url'], 
            report_dict['fast_url_num'], 
            report_dict['slow_url_num'],
            report_dict['same_url_num'],
            report_dict['total_pv'],
            report_dict['fast_pv_num'], 
            report_dict['slow_pv_num'],
            report_dict['same_pv_num'])

    pass

if __name__ == "__main__":
    #parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", dest = "monitor_file_path", required = True, help = "monitor结果文件")
    parser.add_argument("-d", dest = "url_file_path", required = True, help = "url展现信息文件")
    parser.add_argument("-o", dest = "report_file_path", required = True, help = "report内容保存路径")
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

    ret = monitor_parser.load_content_dict(args.monitor_file_path, content_dict)
    if ret != 0:
        sys.exit(1)

    url_dict = {}
    load_url_map(args.url_file_path, url_dict)

    data_config = {}
    data_config['sample'] = {}
    data_config['sample']['target_srcid'] = 6869
    data_config['pad_sample'] = {}
    data_config['pad_sample']['url_param'] = 'dsp=ipad'
    data_config['pad_sample']['target_srcid'] = 6889

    print_udpate_report(args.report_file_path, ["online", "sample"], content_dict, url_dict, data_config)
