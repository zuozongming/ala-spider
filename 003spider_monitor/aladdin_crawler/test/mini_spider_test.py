#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: mini_spider_test.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/19 02:22:27
'''
import unittest

import logging
import sys
sys.path.append('../src/')
import mini_spider

class MiniSpiderTestCase(unittest.TestCase):
    '''
    mini spider test case
    '''

    def setUp(self):
        '''
        test cast set up
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        logger = logging.getLogger("")
        logger.setLevel(logging.DEBUG) 
        formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(process)s ' + 
            '[%(filename)s:%(lineno)s] %(message)s')  

        ch = logging.StreamHandler()  
        ch.setLevel(logging.DEBUG)  
        ch.setFormatter(formatter) 
        logger.addHandler(ch)

        self.spider_config = mini_spider.MiniSpiderConfig(logger)
        pass

    def tearDown(self):
        '''
        test cast tear down
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        pass

    def test_mini_spider(self):
        '''
        test mini spider
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        ret = self.spider_config.load_config("no.confg")
        self.assertNotEqual(ret, 0)


if __name__ == "__main__":
    unittest.main()

