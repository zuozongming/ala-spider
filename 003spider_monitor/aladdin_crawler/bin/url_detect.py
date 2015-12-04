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

    def __init__(self, timeout = None, cookie = None):
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

    def clean_cookie(self):
        '''
        clean cookie
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        self.cookie = cookielib.LWPCookieJar() 

    def download(self, url, data = None):
        '''
        download html page
      
        Args:
            self :
            url :
            data :
        Return: (error_msg, html, redirected_url)
        Raise: 
        '''

        redirected_url = None
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
            error_msg = 'url: %s, '%(url)
            if hasattr(e, 'reason'):
                error_msg += 'network-error: %s'%(e.reason)
            elif hasattr(e, 'code'):
                error_msg += 'server-error, code:%s'%(e.code)
            else:
                error_msg += 'other-error: %s'%(e)
        except KeyboardInterrupt as e:
            error_msg = 'url: %s, '%(url)
            error_msg += 'key board interrupt'
        except:
            error_msg = 'url: %s, '%(url)
            error_msg += 'urlopen-error'
        
        # 3. Read Html 
        if error_msg is not None:
            return error_msg, html, redirected_url
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
        
        return error_msg, html, redirected_url


if __name__ == "__main__":
    downloader = UrlDownloader()
    with open("../conf/wwwus_machine.conf", "r") as f:
        for line in f:
            host = line.strip("\n\r").strip()
            query = "爱情公寓"
            download_url = "http://%s:8012/s?"%(host)
            params = {}
            params['wd'] = query.decode("GB18030").encode("UTF-8")
            params['ie'] = "UTF-8"
            params['tn'] = "SE_testmonitor_4spqbq4b"
            params['sid'] = 1

            download_url += urllib.urlencode(params)
            error_msg, html, redirect_url =  downloader.download(download_url)
            if error_msg is None:
                print "%s:8012" % (host)

#    url = "历劫俏佳人"
#    downloader = UrlDownloader()
#    error_msg, html, redirect_url =  downloader.download(url)
#    print html
#    url = "敢死队2"
#    downloader = UrlDownloader()
#    error_msg, html, redirect_url =  downloader.download(url)
#    print html

    
