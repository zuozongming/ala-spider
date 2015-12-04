#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: aladdin_crawler.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/18 15:14:40
'''
import Queue
import threading
import re
import time
import logging
import argparse
import ConfigParser
import urlparse
import sys
import os

import url_downloader
import url_saver
import base_content_parser

DOWNLOAD_DEFAULT_TIMEOUT = 1
DOWNLOAD_DEFAULT_TIME_INTERVAL = 1
DOWNLOAD_DEFAULT_THREAD_NUM = 8
SAVE_DEFAULT_THREAD_NUM = 8

class DownloadThread(threading.Thread):
    """
    url download thread
    """
    def __init__(self, thread_id, downloader, download_queue, file_queue, config, logger):
        '''
        class constructor
      
        Args:
            self :
            downloader :
            download_queue :
            file_queue :
            config :
            logger :
        Return:   
        Raise: 
        '''

        threading.Thread.__init__(self)
        self._thread_id = "%s_%s"%(os.getpid(), thread_id)
        self._input_queue = download_queue
        self._output_queue = file_queue

        self._downloader = downloader
        self._config = config
        self._link_dupl_map = {}
        self._logger = logger

    def run(self):
        '''
        run job
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        #run job
        while True:
            #time.sleep(self._config.crawl_interval)

            #grab item from job queue
            if self._input_queue is None:
                continue
            task_item = self._input_queue.get()
            if task_item is None:
                self._input_queue.task_done()
                continue

            url = task_item['query']
            data_key = task_item['data_key']
            data_id = task_item['data_id']
            data_idx = int(task_item['data_idx'])
            url_param = task_item['url_param']

            #check if link has been processed
            if url in self._link_dupl_map:
                #signals to queue job is done
                self._input_queue.task_done()
                continue
            #self._link_dupl_map[url] = 1

            #download video result
            error_msg, html = self._downloader.download(url, url_param)
            if None is not error_msg:
                self._logger.warning("data_id[%s] url[%s] downloading is failed, %s" % 
                        (data_id, url, error_msg))
            else:
                self._logger.debug("tid[%s] data_id[%s] url[%s] downloading is ok" % 
                        (self._thread_id, data_id, url))

            if data_idx % 100 == 0:
                self._logger.info("tid[%s] data_id[%s] url[%s] is processed" % 
                        (self._thread_id, data_id, url))

            #check if link content is needed to save
            self._output_queue.put((data_key, url, html))

            #signals to queue job is done
            self._input_queue.task_done()


class SaveThread(threading.Thread):
    """
    save html content thread
    """
    def __init__(self, thread_id, saver, file_queue, crawler_config, logger):
        '''
        class constructor
      
        Args:
            self :
            saver :
            file_queue :
            crawler_config :
            logger :
        Return:   
        Raise: 
        '''

        threading.Thread.__init__(self)
        self._thread_id = "%s_%s"%(os.getpid(), thread_id)
        self._input_queue = file_queue

        self._saver = saver
        self._crawler_config = crawler_config
        self._link_dupl_map = {}
        self._logger = logger

    def run(self):
        '''
        run job
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        while True:
            #grab item from job queue
            if self._input_queue is None:
                continue
            (data_key, url, html) = self._input_queue.get()

            #check if link has been processed
            if url in self._link_dupl_map:
                #signals to queue job is done
                self._input_queue.task_done()
                continue
            #self._link_dupl_map[url] = 1

            #save html to file
            error_msg = self._saver.save(url, html, data_key)
            if None is not error_msg:
                self._logger.warning("url[%s] data_key[%s] saving is failed, %s" % 
                        (url, data_key, error_msg))
            else:
                self._logger.debug("url[%s] data_key[%s] saving is ok" % 
                        (url, data_key))

            #signals to queue job is done
            self._input_queue.task_done()


class AladdinCrawlerConfig:
    '''
    config of mini crawler
    '''
    def __init__(self, logger, 
            query_file_path = None,
            result_file_path = None,
            download_thread_num = DOWNLOAD_DEFAULT_THREAD_NUM, 
            save_thread_num = SAVE_DEFAULT_THREAD_NUM,
            crawl_interval = DOWNLOAD_DEFAULT_TIME_INTERVAL,
            crawl_timeout = DOWNLOAD_DEFAULT_TIMEOUT):
        '''
        class constructor
      
        Args:
            self :
            logger :
            crawl_interval :
            crawl_timeout :
        Return:   
        Raise: 
        '''

        self.download_thread_num = download_thread_num
        self.save_thread_num = save_thread_num
        self.crawl_interval = crawl_interval
        self.crawl_timeout = crawl_timeout

        self.QUERY_CONF_SEC_NAME = "query_config"
        self.TASK_CONF_SEC_NAME = "task_config"
        self.PLUGIN_CONF_SEC_NAME = "plugin"
        self.logger = logger
        self.query_file_path = query_file_path
        self.result_file_path = result_file_path
        self.url_param_dict = {}

        self.query_config = {}

    def load_config(self, conf_path):
        '''
        load crawler config file
      
        Args:
            self :
            conf_path :
        Return: if error return -1 otherwise return 0
        Raise: 
        '''
        #check if config file exists
        if not os.path.isfile(os.path.abspath(conf_path)):
            self.logger.critical("config file[%s] does not exist!" % (conf_path))
            return -1
        config = ConfigParser.ConfigParser()
        config.read(conf_path)

#        query_config_idx = 0
#        query_section_name = "%s_%d"%(self.QUERY_CONF_SEC_NAME, query_config_idx)
#        while config.has_section(query_section_name):
#            conf_item = {}
#
#            if config.has_option(query_section_name, "name") is not True:
#                self.logger.critical("section[%s] has no option[%s]!" % 
#                        (query_section_name, "name"))
#                return -1
#            try:
#                conf_item['name'] = config.get(query_section_name, "name")
#                conf_item['name'] = conf_item['name'].strip()
#                if len(conf_item['name']) == 0:
#                    self.logger.critical("section[%s] option[%s] value[%s] is empty" % 
#                            (query_section_name, "name", conf_item['name']))
#                    return -1
#                if conf_item['name'] in self.query_config:
#                    self.logger.critical("section[%s] option[%s] has duplicate value[%s]" % 
#                            (query_section_name, "name", conf_item['name']))
#                    return -1
#            except ValueError as err:
#                self.logger.critical("section[%s] option[%s] has invalid value" % 
#                        (query_section_name, "name"))
#                return -1
#
#            conf_item['enable'] = 1
#            if config.has_option(query_section_name, "enable") is True:
#                try:
#                    conf_item['enable'] = config.getint(query_section_name, "enable")
#                except ValueError as err:
#                    self.logger.critical("section[%s] option[%s] has invalid value" % 
#                            (query_section_name, "enbale"))
#                    return -1
#
#            conf_item['url_param'] = []
#            if config.has_option(query_section_name, "url_param") is True:
#                url_param_str = ""
#                try:
#                    url_param_str = config.get(query_section_name, "url_param")
#                    params = urlparse.parse_qsl(url_param_str, 
#                            keep_blank_values=False, strict_parsing=True)
#                    conf_item['url_param'] = params
#                except ValueError as err:
#                    self.logger.critical("section[%s] option[%s] has invalid value[%s]" % 
#                            (query_section_name, "url_param", url_param_str))
#                    return -1
#
#            conf_key = conf_item['name']
#            self.query_config[conf_key] = conf_item
#
#            query_config_idx += 1
#            query_section_name = "%s_%d"%(self.QUERY_CONF_SEC_NAME, query_config_idx)
        #load config
        try:
            if config.has_option(self.TASK_CONF_SEC_NAME, "url_param") is True:
                url_param_str = config.get(self.TASK_CONF_SEC_NAME, "url_param")
                params = urlparse.parse_qsl(url_param_str, 
                            keep_blank_values=False, strict_parsing=True)
                for t_k, t_v in enumerate(params):
                    self.url_param_dict[t_v[0]] = t_v[1]

            if self.query_file_path is None:
                self.query_file_path = config.get(self.TASK_CONF_SEC_NAME, "query_file_path")
            #check if file exists
            if not os.path.isfile(self.query_file_path):
                self.logger.critical("path[%s] %s file does not exist!" % 
                        (self.query_file_path, "query_file_path"))
                return -1

            self.site_file_path = config.get(self.TASK_CONF_SEC_NAME, "site_file_path")
            self.logger.critical("path %s file does exist!" % self.site_file_path)
            #check if file exists
            if not os.path.isfile(self.site_file_path):
                self.logger.critical("path[%s] %s file does not exist!" % 
                        (self.site_file_path, "site_file_path"))
                return -1
            if self.result_file_path is None:
                self.result_file_path = config.get(self.TASK_CONF_SEC_NAME, "result_file_path")
            try:
                f = open(self.result_file_path, 'w')
                f.close()
            except IOError as err:
                self.logger.critical("fail to write file[%s] error: %s" %
                        (self.result_file_path, err))
                return -1

            #check value type
            try:
                self.download_thread_num = config.getint(self.TASK_CONF_SEC_NAME, "download_thread_count")
            except ValueError as err:
                self.logger.critical("load config file[%s] prop[%s] error: %s" %
                        (conf_path, "download_thread_count", err))
                return -1

            #check value type
            try:
                self.save_thread_num = config.getint(self.TASK_CONF_SEC_NAME, "save_thread_count")
            except ValueError as err:
                self.logger.critical("load config file[%s] prop[%s] error: %s" %
                        (conf_path, "save_thread_count", err))
                return -1

            #check value type
            try:
                self.crawl_interval = config.getint(self.TASK_CONF_SEC_NAME, "crawl_interval")
            except ValueError as err:
                self.logger.critical("load config file[%s] prop[%s] error: %s" %
                        (conf_path, "crawl_interval", err))
                return -1

            #check value type
            try:
                self.crawl_timeout = config.getint(self.TASK_CONF_SEC_NAME, "crawl_timeout")
            except ValueError as err:
                self.logger.critical("load config file[%s] prop[%s] error: %s" % 
                        (conf_path, "crawl_timeout", err))
                return -1

            if config.has_option(self.PLUGIN_CONF_SEC_NAME, "content_parser_module") is True:
                parser_module_name = config.get(self.PLUGIN_CONF_SEC_NAME, "content_parser_module")
                parser_class_name = config.get(self.PLUGIN_CONF_SEC_NAME, "content_parser_class")
                try:
                    module = __import__(parser_module_name)
                    content_parser = getattr(module, parser_class_name)
                    self.content_parser = content_parser()
                except Exception as e:
                    self.logger.fatal("load %s from %s failed: %s", 
                            parser_class_name, parser_module_name, str(e))
                    return -1
            else:
                self.content_parser =  base_content_parser.BaseContentParser()

        except KeyError as err:
            self.logger.critical("load config file[%s] error: %s" %
                    (conf_path, err))
            return -1
        except ValueError as err:
            self.logger.critical("load config file[%s] error: %s" % (conf_path, err))
            return -1
        except ConfigParser.NoOptionError as err:
            self.logger.critical("load config file[%s] error: %s" % (conf_path, err))
            return -1
        except ConfigParser.NoSectionError as err:
            self.logger.critical("load config file[%s] error: %s" % (conf_path, err))
            return -1

        return 0


class AladdinCrawler:
    '''
    mini crawler 
    '''
    def __init__(self, crawler_config, logger):
        '''
        class constructor
      
        Args:
            self :
            crawler_config :
            logger :
        Return:   
        Raise: 
        '''

        #breadth first traversal
        self._download_queue = Queue.PriorityQueue()
        self._save_queue = Queue.Queue()
        self._config = crawler_config
        self._downloader = url_downloader.UrlDownloader(self._config.content_parser, self._config.site_file_path, 
                self._config.crawl_timeout, self._config.crawl_interval)
        self._saver = url_saver.UrlSaver(self._config.result_file_path)
        self._logger = logger

    def set_config(crawler_config):
        '''
        set crawler config
      
        Args:
            crawler_config :
        Return:   
        Raise: 
        '''

        self._config = crawler_config

    def get_config():
        '''
        get config
      
        Args:
        Return:   
        Raise: 
        '''

        return self._config

    def run(self):
        '''
        run crawler 
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        self._logger.info("crawler starts crawling")

        query_file = open(self._config.query_file_path, "r")
        line_counter = 0
        counter = 0
        for line in query_file:
            #if line_counter > 10:
            #    break
            line_counter += 1
            line = line.rstrip("\r\n").rstrip()
            cols = line.split("\t")
            col_cur_idx = 0
            query = cols[col_cur_idx]
            col_cur_idx += 1
            if len(query) == 0:
                self._logger.warning("empty query at line[%d] file[%s]" % 
                        (line_counter, self._config.query_file_path))
                continue
            

            if len(cols) == 1:
                #deep copy
                url_param_dict = {}
                for param_k, param_v in self._config.url_param_dict.iteritems():
                    url_param_dict[param_k] = param_v

                queue_item = {}
                queue_item['query'] = query
                queue_item['url_param'] = url_param_dict
                queue_item['url_param']['sid'] = 1
                queue_item['data_key'] = "online"
                queue_item['data_id'] = ("%s_%d_%d_%d" % 
                        (self._config.query_file_path, line_counter, counter, queue_item['data_key']))
                queue_item['data_idx'] = counter
                self._download_queue.put(queue_item)
                self._logger.debug("add seed url[%s] data_key[%s]" % (query, queue_item['data_key']))
                counter += 1
                continue
            item_num = int(cols[col_cur_idx])
            col_cur_idx += 1
            item_start_idx = col_cur_idx
            prop_num = 2

            col_end_idx = item_start_idx + (item_num * prop_num)


            if len(cols) != col_end_idx:
                self._logger.warning("invalid item_num[%d] url[%s] data_key[%s]" % 
                        (item_num, query, queue_item['data_key']))
                continue
            for idx in range(0, item_num):
                #deep copy
                url_param_dict = {}
                for param_k, param_v in self._config.url_param_dict.iteritems():
                    url_param_dict[param_k] = param_v

                data_key = cols[col_cur_idx]
                url_param_str = cols[col_cur_idx + 1]
                col_cur_idx += 2

                url_params = urlparse.parse_qsl(url_param_str, 
                            keep_blank_values=False, strict_parsing=True)
                for param_v in url_params:
                    url_param_dict[param_v[0]] = param_v[1]

                queue_item = {}
                queue_item['query'] = query
                queue_item['url_param'] = url_param_dict
                queue_item['data_key'] = data_key
                queue_item['data_id'] = ("%s_line=%d_idx=%d_%s" % 
                        (self._config.query_file_path, line_counter, counter, queue_item['data_key']))
                queue_item['data_idx'] = counter
                self._download_queue.put(queue_item)
                self._logger.debug("add seed url[%s] data_key[%s]" % 
                        (query, queue_item['data_key']))
                counter += 1

        for i in range(self._config.download_thread_num):
            t = DownloadThread(i, self._downloader, 
                    self._download_queue, self._save_queue, 
                    self._config, self._logger)
            t.setDaemon(True)
            t.start()
        self._logger.debug("%d download threads are started" % (self._config.download_thread_num))

        for i in range(self._config.save_thread_num):
            t = SaveThread(i, self._saver, self._save_queue, self._config, self._logger)
            t.setDaemon(True)
            t.start()
        self._logger.debug("%d save threads are started" % (self._config.save_thread_num))    

        #最后加点负载空转，避免队列获取不到数据抛异常
        for idx in range(0, 5 * self._config.download_thread_num):
            self._download_queue.put(None)

        self._download_queue.join()
        self._save_queue.join()
        self._logger.info("crawler finishes crawling")


if __name__ == "__main__":
    #parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", dest = "conf_path", required = True, help = "crawler配置文件路径")
    parser.add_argument("-q", dest = "query_path", required = False, help = "query文件路径")
    parser.add_argument("-o", dest = "output_path", required = False, help = "输出文件路径")
    args = parser.parse_args()
    #返回当前文件路径 去文件名返回目录路径
    script_base_path = os.path.dirname(os.path.realpath(__file__))

    sys.path.append(script_base_path + "/../plugin/")
    sys.path.append(script_base_path + "/../lib/")

    #logging
    log_path = script_base_path
    log_path += "/../log/"
    log_name = "crawl"
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO) 
    #logger.setLevel(logging.DEBUG) 

    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(process)s ' + 
        '[%(filename)s:%(lineno)s] %(message)s')  

    fh = logging.FileHandler(log_path + log_name + ".log")  
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter) 
    logger.addHandler(fh)

    #load config file
    crawler_config = AladdinCrawlerConfig(logger, args.query_path, args.output_path)
    ret = crawler_config.load_config(args.conf_path)
    if 0 != ret:
        logger.critical("fail to load config: %s" % (args.conf_path))
        sys.exit(1)

    #crawler starts running
    crawler = AladdinCrawler(crawler_config, logger)
    crawler.run()
