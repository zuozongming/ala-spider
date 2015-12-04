#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: url_downloader.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/18 14:52:49
'''
import logging
import urllib2
import urllib
import cookielib
import socket
import time
import random
import json

SOCKET_DEFAULT_TIMEOUT = 30

class UrlDownloader:
    '''
    url download
    '''

    def __init__(self, content_parser, site_file_path, 
            timeout = None, retry_interval = 1, retry_times = 5, cookie = None):
        '''
        class constructor
      
        Args:
            self :
            timeout :
            cookie :
        Return:   
        Raise: 
        '''
        # cookie
        if cookie is None:
            self.cookie = cookielib.LWPCookieJar() 
        else:
            self.cookie = cookie
        opener = urllib2.build_opener(urllib2.HTTPRedirectHandler, 
                urllib2.HTTPCookieProcessor(self.cookie))
        urllib2.install_opener(opener)
        # socket timeout
        if timeout is None:
            timeout = SOCKET_DEFAULT_TIMEOUT
            socket.setdefaulttimeout(timeout)
        self.content_parser = content_parser

        self.retry_times = retry_times
        self.retry_interval = retry_interval
        self.host_list = []
        with open(site_file_path, "r") as f:
            for line in f:
                line = line.strip("\n\r").strip()
                self.host_list.append(line)
                
    def clean_cookie(self):
        '''
        clean cookie
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        self.cookie = cookielib.LWPCookieJar() 

    def download(self, url, url_data = None):
        '''
        download html page
      
        Args:
            self :
            url :
            data :
        Return: (error_msg, html)
        Raise: 
        '''

        query = url

        error_msg = None
        html = None
        result = {}
        result['query'] = query
        result['sample_id'] = url_data['sid']

        retry_num = self.retry_times

        result_data = {}
        for idx in range(0, retry_num):
            if idx > 0 and self.retry_interval > 0:
                time.sleep(self.retry_interval)
                pass
            error_msg = None
            
            params = {}
            if url_data is not None:
                params = url_data
            params['wd'] = query.decode("GB18030").encode("UTF-8")
            params['ie'] = "UTF-8"
            '''
            if "domain" in params:
                replace
                del params[domain]
            '''
            if url_data['sid'] == '0000':
                #host = "cq01-2011q4-setest3-17.vm.baidu.com:8012"
                host = "cp01-ps-diaoyan24.cp01.baidu.com:8201"
            else:
                host_idx = random.randrange(0, len(self.host_list))
                host = self.host_list[host_idx]
            download_url = "http://%s/s?"%(host)
            download_url += urllib.urlencode(params)
            error_msg, html = self._download(download_url)
            if error_msg is not None:
                continue
            error_msg, result_data = self.content_parser.parse(html) 
            break
        result['error_msg'] = error_msg
        result['data'] = result_data

        return error_msg, result

    def _download(self, url, data = None):
        '''
        download html page
      
        Args:
            self :
            url :
            data :
        Return: (error_msg, html)
        Raise: 
        '''

        error_msg = None
        html = None
        # 1. URL Request Head
        user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
        headers = { 'User-Agent' : user_agent }
        if data is not None:
            req = urllib2.Request(url, urllib.urlencode(data), headers)
        else:
            req = urllib2.Request(url, data, headers)
        # 2. URL Request
        try:
            response = urllib2.urlopen(req)
        except urllib2.URLError as e:
            error_msg = 'url[%s], '%(url)
            if hasattr(e, 'reason'):
                error_msg += 'network-error: %s'%(e.reason)
            elif hasattr(e, 'code'):
                error_msg += 'server-error, code:%s'%(e.code)
            else:
                error_msg += 'other-error: %s'%(e)
        except urllib2.HTTPError as e:
            error_msg = 'url[%s], '%(url)
            if hasattr(e, 'reason'):
                error_msg += 'network-error: %s'%(e.reason)
            elif hasattr(e, 'code'):
                error_msg += 'server-error, code:%s'%(e.code)
            else:
                error_msg += 'other-error: %s'%(e)
        except socket.error as e:
            error_msg = 'url[%s], '%(url)
            if hasattr(e, 'reason'):
                error_msg += 'network-error: %s'%(e.reason)
            elif hasattr(e, 'code'):
                error_msg += 'server-error, code:%s'%(e.code)
            else:
                error_msg += 'other-error: %s'%(e)
        except KeyboardInterrupt as e:
            error_msg = 'url: %s, '%(url)
            error_msg += 'key board interrupt'
        #except:
        #    error_msg = 'url: %s, '%(url)
        #    error_msg += 'urlopen-error'
        
        # 3. Read Html 
        if error_msg is not None:
            return error_msg, html
        try:
            html = response.read()
        except KeyboardInterrupt as e:
            error_msg = 'url: %s, '%(url)
            error_msg += 'key board interrupt'
        except:
            error_msg = 'url: %s, '%(url)
            error_msg += 'reading-error'

        nurl = response.geturl()
        if url != nurl:
            redirected_url = nurl
        else:
            redirected_url = None            
        
        return error_msg, html


if __name__ == "__main__":
    url = "爱情公寓"
    downloader = UrlDownloader()
    error_msg, html, redirect_url =  downloader.download(url)
    print html
#!!
    log_path = "../log/"
    log_name = "url_download"
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
  
    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(process)s ' +
            '[%(filename)s:%(lineno)s] %(message)s')
    fh = logging.FileHandler(log_path + log_name + ".log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
  #  url = "历劫俏佳人"
  #  downloader = UrlDownloader()
  #  error_msg, html, redirect_url =  downloader.download(url)
  #  print html
  #  url = "敢死队2"
  #  downloader = UrlDownloader()
  #  error_msg, html, redirect_url =  downloader.download(url)
  #  print html

    
