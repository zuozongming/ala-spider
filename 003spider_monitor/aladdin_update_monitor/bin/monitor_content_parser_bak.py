#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
########################################################################

"""
File: monitor_content_parser.py
Author: yudonglee1(yudonglee@baidu.com)
Date: 2014/11/13 11:39:11
"""
import datetime
import json

class MonitorContentParser:
    def __init__(self, logger):
        self._logger = logger

    def do_diff_item(self, old_item, new_item, key_list):
        diff_result_list = []
        if new_item['error_msg'] != old_item['error_msg']:
            if new_item['error_msg'] is None:
                diff_item = [1, "抓取恢复", ""]
                diff_result_list.append(diff_item)
                return diff_result_list
            diff_item = [1, "抓取失败", new_item['error_msg']]
            diff_result_list.append(diff_item)
            return diff_result_list
        if (len(new_item['data']) == 0 and len(old_item['data']) != 0):
            diff_item = [2, "未展现", "结果不展现"]
            diff_result_list.append(diff_item)
            return diff_result_list
        elif (len(new_item['data']) != 0 and len(old_item['data']) == 0):
            diff_item = [2, "恢复展现", "结果恢复展现"]
            diff_result_list.append(diff_item)
            return diff_result_list
        elif (len(new_item['data']) == 0 and len(old_item['data']) == 0):
            return diff_result_list

        for key in key_list:
            if old_item['data'][key] != new_item['data'][key]:
                diff_item = [3, "%s字段变动" % (key), "new_%s[%s] old_%s[%s]" %
                                (key, old_item['data'][key], key, new_item['data'][key]), key]
                diff_item[2] = diff_item[2].encode("GB18030")
                diff_result_list.append(diff_item)
        if 'ext_data' in new_item and 'ext_data' not in old_item and len(diff_result_list) == 0:
            diff_item = [4, "ext_data", "新增ext_data字段"]
            diff_result_list.append(diff_item)

        if 'ext_data' in new_item and 'ext_data' in old_item:
            for k, v in new_item['ext_data']:
                if k in old_item['ext_data'] and old_item['ext_data'][k] != new_item['ext_data'][k]:
                    diff_item = [5, "ext_data.%s字段变动" % (k),
                        "new_%s[%s] old_%s[%s]" % (k, old_item['ext_data'][k], k, k), key]
                    diff_item[2] = diff_item[2].encode("GB18030")
                    diff_result_list.append(diff_item)
        return diff_result_list

    def parse_item(self, query, raw_item):
        result = {}
        cur_time = datetime.datetime.now()
        if raw_item is None:
            return None
        if len(raw_item) == 0:
            return None

        for item_k, item_v in raw_item.iteritems():
            result[item_k] = {}
            for k, v in item_v.iteritems():
                if k == 'data':
                    continue
                result[item_k][k] = v
            if len(item_v['data']) == 0:
                result[item_k]['data'] = {}
                continue
            #query.decode("UTF-8").encode("GB18030")

            item_data = {}
            item_data['content_key'] = None
            item_data['url'] = item_v['data']['url']
            item_data['title'] = item_v['data']['title']
            item_data['position'] = item_v['data']['position']
            item_data['srcid'] = int(item_v['data']['srcid'])
            item_data['tplt'] = item_v['data']['tplt']
            item_data['update_time'] = cur_time.strftime('%Y-%m-%d %H:%M:%S')

            tpl_data = item_v['data']['tplData']
            if item_data['srcid'] == 6869 or item_data['srcid'] == 6889:
                child_result = tpl_data['result'][0]
                item_data['title'] = child_result['title']
                item_data['url'] = child_result['url']

                item_data['type'] = None
                item_data['latest_episode'] = ""
                item_data['total_episode'] = ""
                item_data['is_end'] = 0
                if child_result['se_class'] == "动漫".decode("GB18030"):
                    item_data['type'] = "comic"
                    item_data['latest_episode'] = child_result['latestEp']
                    if 'totalEp' in child_result:
                        item_data['total_episode'] = child_result['totalEp']
                elif child_result['se_class'] == "电视剧".decode("GB18030"):
                    item_data['type'] = "tv"
                    item_data['latest_episode'] = child_result['latestEp']
                    if 'totalEp' in child_result:
                        item_data['total_episode'] = child_result['totalEp']
                elif child_result['se_class'] == "电影".decode("GB18030"):
                    item_data['type'] = "movie"
                elif child_result['se_class'] == "综艺".decode("GB18030"):
                    item_data['type'] = "zongyi"
                    item_data['latest_episode'] = child_result['newest_episode']

                #综艺模版
                if 'newest_episode' in child_result:
                    item_data['content_key'] = ("%s" % (child_result['newest_episode']))
                #动漫或电视剧
                elif 'latestEp' in child_result:
                    if 'totalEp' not in child_result or len(child_result['totalEp']) == 0:
                        item_data['content_key'] = ("%s" %  (child_result['latestEp']))
                    else:
                        item_data['content_key'] = ("%s/%s" %
                                (child_result['latestEp'], child_result['totalEp']))
                #电影
                elif 'zx_new_mvideo' == item_data['tplt']:
                    if 'se_director' in child_result:
                        item_data['content_key'] = child_result['se_director']
                    else:
                        item_data['content_key'] = "movie"
                    if 'se_year' in child_result:
                        item_data['content_key'] += ("_%s" % child_result['se_year'])
                    if 'se_area' in child_result:
                        item_data['content_key'] += ("_%s" % child_result['se_area'])
                    if 'se_lang' in child_result:
                        item_data['content_key'] += ("_%s" % child_result['se_lang'])

                item_data['ext_data'] = {}
                item_data['ext_data']['uri'] = child_result['se_uri']
                item_data['ext_data']['class'] = child_result['se_class']
                item_data['ext_data']['site'] = "|"

                for child_result in tpl_data['result']:
                    item_data['ext_data']['site'] += child_result['se_sitename']
                    item_data['ext_data']['site'] += "|"
            else:
                item_data['type'] = None
                item_data['latest_episode'] = ""
                item_data['total_episode'] = ""
                item_data['is_end'] = 0

                #综艺模版
                if item_data['tplt'] == 'vd_tamasha':
                    item_data['type'] = "zongyi"
                    if 'newest_episode' in tpl_data:
                        item_data['content_key'] = "%s" % (tpl_data['newest_episode'])
                        item_data['latest_episode'] = tpl_data['newest_episode']
                    elif 'result' in tpl_data:
                        child_result = tpl_data['result'][0]
                        item_data['content_key'] = "%s" % (child_result['newest_episode'])
                        item_data['latest_episode'] = child_result['newest_episode']
                    else:
                        tpl_data['newest_episode']
                #电视剧
                elif item_data['tplt'] == 'zx_tv_video':
                    if (item_data['srcid'] == 15857 or item_data['srcid'] == 13774 or
                            item_data['srcid'] == 12259 or item_data['srcid'] == 13717):
                        item_data['type'] = "tv"
                    elif item_data['srcid'] == 14582 or item_data['srcid'] == 13694:
                        item_data['type'] = "comic"
                    if len(tpl_data['epCount']) > 0:
                        item_data['content_key'] = "%s/%s" % (tpl_data['latestEp'], tpl_data['epCount'])
                    else:
                        item_data['content_key'] = "%s" % (tpl_data['latestEp'])
                    item_data['latest_episode'] = tpl_data['latestEp']
                    item_data['total_episode'] = tpl_data['epCount']
                elif item_data['tplt'] == 'vd_sitcom':
                    if len(tpl_data['max_episode']) > 0:
                        item_data['content_key'] = "%s/%s" % (tpl_data['cur_episodes'], tpl_data['max_episode'])
                    else:
                        item_data['content_key'] = "%s" % (tpl_data['cur_episodes'])
                    item_data['latest_episode'] = tpl_data['cur_episodes']
                    item_data['latest_episode'] = tpl_data['max_episode']
                elif 'movievideo' == item_data['tplt']:
                    if (item_data['srcid'] == 15428 or item_data['srcid'] == 11834 or
                            item_data['srcid'] == 13792):
                        item_data['type'] = "movie"
                    item_data['content_key'] = tpl_data['releaseTime']
                    if 'area' in tpl_data:
                        item_data['content_key'] += "_%s" % tpl_data['area']
                    item_data['content_key'] += "_%s" % tpl_data['duration']
                    pass
                elif 'vd_comic' == item_data['tplt']:
                    item_data['type'] = "comic"

                    if int(tpl_data['finish']) != 0:
                        item_data['content_key'] = tpl_data['max_episode']
                        item_data['latest_episode'] = tpl_data['max_episode']
                        item_data['total_episode'] = tpl_data['max_episode']
                    else:
                        item_data['content_key'] = "%s/%s" % (tpl_data['cur_episodes'], tpl_data['max_episode'])
                        item_data['latest_episode'] = tpl_data['cur_episodes']
                        item_data['total_episode'] = tpl_data['max_episode']

                    #item_data['latest_episode'] = tpl_data['latestEp']
                    pass
                if tpl_data['url'].find("comic") > 0:
                    item_data['type'] = "comic"
                elif tpl_data['url'].find("tv") > 0:
                    item_data['type'] = "tv"
                elif tpl_data['url'].find("movie") > 0:
                    item_data['type'] = "movie"
                elif tpl_data['url'].find("show") > 0:
                    item_data['type'] = "zongyi"

            if item_data['content_key'] is None:
                print "no content key", item_data['tplt'], item_data['srcid'], item_data['title'].encode("GB18030"), query.encode("GB18030"), item_v, item_data
                continue
            if item_data['type'] is None:
                print "no type", item_data['tplt'], item_data['srcid'], query.encode("GB18030"), item_v, item_data
                continue
            result[item_k]['data'] = item_data
        return result
    def load_content_dict(self, save_dict_path, content_dict):
        if content_dict is None:
            return -1
        try:
            fsock = open(save_dict_path, "r", 0)
        except IOError:
            return 0
        line_counter = 0
        for line in fsock:
            line_counter += 1
            line = line.rstrip("\r\n ")
            cols = line.split("\t")
            query = cols[0]
            item_k = cols[1]
            json_str = cols[2]

            query = query.decode("UTF-8")
            item_k = item_k.decode("UTF-8")
            item_v = json.loads(json_str)

            if query not in content_dict:
                content_dict[query] = {}

            content_dict[query][item_k] = item_v
        return 0

    def add_content_dict(self, crawler_result_path, content_dict):
        line_counter = 0
        crawler_result_handler = open(crawler_result_path, "r")

        for line in crawler_result_handler:
            line_counter += 1
            line = line.rstrip("\r\n ")
            cols = line.split("\t")
            query = cols[0]
            query = query.decode("GB18030")
            json_str = cols[1]
            raw_item = json.loads(json_str)
            item = self.parse_item(query, raw_item)
            for item_k, item_v in item.iteritems():
                if len(item_v['data']) == 0:
                    print cols[0], "not display"
                pass
                if query not in content_dict:
                    content_dict[query] = {}
                if item_k not in content_dict[query]:
                    content_dict[query][item_k] = []
                    content_dict[query][item_k].append(item_v)
                else:
                    diff_key_list = [
                            'url',
                            'srcid',
                            'title',
                            'content_key',
                            'position',
                            'type',
                            'tplt'
                            ]
                    diff_list = self.do_diff_item(content_dict[query][item_k][-1], item_v, diff_key_list)
                    for idx, diff_item in enumerate(diff_list):
                        print diff_item[1]
                        print diff_item[2]
                        diff_list[idx][1] = diff_item[1].decode("GB18030")
                        diff_list[idx][2] = diff_item[2].decode("GB18030")
                    if len(diff_list) > 0:
                        item_v['diff_list'] = diff_list
                        content_dict[query][item_k].append(item_v)
                #TO DO
                #truncation of content_dict[query][item_k]
        return 0

    def save_content_dict(self, save_dict_path, content_dict):
        if content_dict is None:
            return -1
        try:
            fsock = open(save_dict_path, "w", 0)
        except IOError:
            return -1
        for query, content_item in content_dict.iteritems():
            for item_k, item_v in content_item.iteritems():
                json_str = json.dumps(item_v)
                fsock.write("%s\t%s\t%s\n" % (query.encode("UTF-8"), item_k.encode("UTF-8"), json_str))
        fsock.close()
        return 0
