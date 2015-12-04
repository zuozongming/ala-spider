#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: html_parser.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/18 12:00:58
'''

import urlparse
import re

import lxml.html

class HtmlParser:
    '''
    html parser
    '''
    def __init__(self, tags= [], patterns = []):
        '''
        class constructor
      
        Args:
            self :
            tags :
            patterns :
        Return:   
        Raise: 
        '''

        self._patterns = patterns
        self._tags = tags

    def __parse_links(self, url, doc):
        '''
        parse html link
      
        Args:
            self :
        Return:   
        Raise: 
        '''
        links = {}

        for elem, attr, link, pos in doc.iterlinks():
            absolute = urlparse.urljoin(url, link.strip())
            if elem.tag in links:
                links[elem.tag].append(absolute)
            else:
                links[elem.tag] = [absolute]
        return links

    def filter_links(self, url, html):
        '''
        filter link by parameters
      
        Args:
            self :
            tags : filter by html tag name
            patterns : filter by link pattern
        Return:   
        Raise: 
        '''


        try:
            doc = lxml.html.fromstring(html)
        except lxml.etree.XMLSyntaxError as e:
            return []
        except lxml.etree.ParserError as e:
            return []

        links = self.__parse_links(url, doc)

        filterlinks = []
        #filter by tag
        if len(self._tags) > 0:
            for tag in self._tags:
                for link in links[tag]:
                    #if no pattern specified, then link matches
                    if len(self._patterns) == 0:
                        filterlinks.append(link)
                    else:
                        #filter by pattern
                        for pattern in self._patterns:
                            reg = re.compile(pattern)
                            if reg.match(link) is not None:
                                filterlinks.append(link)
                                continue
        else:
            for k,v in links.items():
                for link in v:
                    #if no pattern specified, then link matches
                    if len(self._patterns) == 0:
                        filterlinks.append(link)
                    else:
                        #filter by pattern
                        for pattern in self._patterns:
                            reg = re.compile(pattern)
                            if reg.match(link) is not None:
                                filterlinks.append(link)
                                continue
        #remove duplicate result
        return list(set(filterlinks))

    def get_html(self):
        '''
        get html raw content
      
        Args:
            self :
        Return:   
        Raise: 
        '''
        return self.html


if __name__ == "__main__":
    import time
    import url_downloader
    downloader = url_downloader.UrlDownloader()

    url = "http://desk.zol.com.cn/"
    error_msg, html, redirected_url = downloader.download(url)
    print error_msg, url, redirected_url, len(html)
    time.sleep(2)

    page = HtmlParser([], ['.*\.(gif|png|jpg|bmp)$'])
    links = page.filter_links(url, html)
    print links
