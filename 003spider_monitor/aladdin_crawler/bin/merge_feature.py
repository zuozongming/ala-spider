#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
"""
File: feature_merge.py
Author: liupu(liupu@baidu.com)
Date: 2014/09/08 16:36:47
"""

import ConfigParser
import os
import sys
import logging

import get_dependency
import aladreqident_common
import feature_merger
import hadoop_job

class MergeFeature(hadoop_job.HadoopJob):
    def __init__(self, task_name):
        """��ʼ������"""
        super(MergeFeature, self).__init__(task_name)
        self.__merge_func = {}
        self.__custom_merge = {}
        self.__depend_files = set()
        self.__bin_path = os.path.dirname(os.path.realpath(__file__))
        self.__so_path = os.path.join(self.__bin_path, "../so/")
        #logging = logging.getLogger()

    @property
    def depend_files(self):
        return self.__depend_files

    def __load_custom_feature_merger(self, conf_parser, cate_id):
        """
        @brief
            �����Զ��������ϲ�so
        @param
            conf_parser     ConfigParser.ConfigParser
            cate_id         str         ��Ŀid
        @retval
            True
            False
        """
        class_name = aladreqident_common.get_unnull_value(conf_parser, "feature_merger", "name")
        so_file = aladreqident_common.get_unnull_value(conf_parser, "feature_merger", "so_file")
        # ���ģ�����������ļ�
        so_file_path = os.path.join(self.__so_path, so_file)
        dp_file_list = get_dependency.get_depend_file(aladreqident_common.get_path(so_file_path))
        self.__depend_files.update(dp_file_list)
        self.__depend_files.add(so_file_path)
        # ��ʼ����
        module_name = os.path.splitext(os.path.basename(so_file_path))[0]
        try:
            module = __import__(module_name)
            merge_class = getattr(module, class_name)
        except Exception as e:
            logging.fatal("load %s from %s failed: %s", class_name, module_name, str(e))
            return False
        self.__custom_merge[cate_id] = merge_class()
        """
        try:
            merge_class = getattr(custom_merge, f_merger)
        except Exception as e:
            logging.fatal("load %s from custom_merge failed", f_merger)
            return False
        self.__custom_merge[cate_id] = merge_class()
        if not self.__custom_merge[cate_id].load_conf(conf_parser):
            logging.fatal("cate_id: %s custom merger load config file failed", cate_id)
            return False
        """
        return True

    def _load_category_conf(self, conf_path, cate_name):
        """
        @brief
            ������Ŀ����
        @param
            conf_path   �����ļ���·��  str
            cate_name   ��Ŀ��          str
        @retval
            True
            False
        """
        #conf_name = "%s/%s_feature.conf" % (conf_path, cate_name)
        conf_parser = ConfigParser.ConfigParser()
        try:
            conf_parser.read(aladreqident_common.get_path(conf_path))
        except Exception as e:
            logging.fatal("read config file for category %s failed", cate_name)
        cate_id = aladreqident_common.get_conf_value(conf_parser, "CategoryInfo", "category_id")
        if cate_id is None or len(cate_id) == 0:
            logging.fatal("CategoryInfo/category_id has not been configured for %s", cate_name)
            return False

        # check�Ƿ�Ϊ�Զ���feature merge��
        custom_feature = aladreqident_common.get_conf_value(conf_parser, "CategoryInfo", "custom_feature", "0")
        if custom_feature == "1":
            return self.__load_custom_feature_merger(conf_parser, cate_id)

        feature_list = aladreqident_common.get_conf_value(conf_parser, "FeatureList", "name")
        if feature_list is None or len(feature_list) == 0:
            logging.fatal("category %s not configure FeatureList/name", cate_name)
            return False

        feature_list = feature_list.split(",")
        for feature_name in feature_list:
            feature_name = feature_name.strip()
            order = aladreqident_common.get_conf_int(conf_parser, feature_name, "order")
            merge_method = aladreqident_common.get_conf_value(conf_parser, feature_name, "merge_method")
            try:
                merge_func = getattr(feature_merger, merge_method)
            except Exception as e:
                logging.fatal("get merge func for %s failed", merge_method)
                return False
            if cate_id not in self.__merge_func:
                self.__merge_func[cate_id] = {}
            order -= 1
            self.__merge_func[cate_id][order] = merge_func
        return True

    def load_conf(self, conf_file):
        """
        @brief
            ��������
        @param
            conf_file   �����ļ���  str
        @retval
            True
            False
        """
        conf_parser = ConfigParser.ConfigParser()
        conf_parser.read(aladreqident_common.get_path(conf_file))

        category_list = aladreqident_common.get_conf_value(conf_parser, "CategoryList", "name")
        if category_list is None or len(category_list) == 0:
            logging.fatal("CategoryList/name has not been configured")
            return False
        category_list = category_list.split(",")
        conf_path = os.path.dirname(conf_file)
        for category in category_list:
            category = category.strip()
            if len(category) == 0:
                continue
            cate_conf = aladreqident_common.get_unnull_value(conf_parser, category, "conf")
            if len(cate_conf) == 0:
                logging.fatal("cate [%s] conf not configured", category)
                return False
            cate_conf = os.path.join(self.task_space, cate_conf)
            if not self._load_category_conf(cate_conf, category):
                logging.fatal("load category %s failed", category)
                return False
        return True

    def mapper(self, fin, fout, **kwargs):
        """
        @brief
            ����merge��map����
            ����hadoop��shuffle���������, ����ͬquery�����ݾ缯��һ��
        """
        cmd = "cat -"
        os.system(cmd)
        return True

    def reducer(self, fin, fout, **kwargs):
        """
        @brief
            ����merge��reduce����
            reduce�׶���Ҫ�������ǽ�ͬһ��query��, ͬһ����Ŀ�ĸ�������merge��һ��, �����������ļ���˳�����.
            ����ĸ�ʽΪ query \t f1 \t f2 ... \t fn \t cate_id
        @param
            fin     �����
            fout    �����
            kwargs  ����Ҫ�Ķ������
        @retval
            True
            False
        """
        pre_query = ""
        cate_feature = {}
        for cate_id in self.__merge_func:
            cate_feature[cate_id] = ["" for i in range(0, len(self.__merge_func[cate_id]))]

        for line in fin:
            line = line.strip("\r\n")
            if len(line) == 0:
                continue
            cols = line.split("\t")
            query = cols[0]
            cate_id = cols[-1]
            
            if cate_id in self.__custom_merge:
                result = self.__custom_merge[cate_id].merge_feature(cols[:-1])
                if not result is None:
                    result = [str(item) for item in result]
                    print >> fout, "%s\t%s" % ("\t".join(result), cate_id)
                continue
            if query != pre_query:
                if pre_query != "":
                    for (cid, flist) in cate_feature.iteritems():
                        out_str = pre_query
                        for f in flist:
                            if f == "":
                                out_str += "\t-"
                            else:
                                out_str += "\t%s" % (f)
                        print >> fout, "%s\t%s" % (out_str, cid)
                        cate_feature[cid] = ["" for i in range(0, len(self.__merge_func[cid]))]
                pre_query = query
                # cate_feature[cate_id] = ["" for i in range(0, len(self.__merge_func[cate_id]))]
            for f_order in cols[1:-1]:
                (f, order) = f_order.split(":")
                try:
                    order = int(order)
                    order -= 1
                except Exception as e:
                    logging.fatal("get order from [%s] failed: %s", f_order, str(e))
                    return False
                cate_feature[cate_id][order] = self.__merge_func[cate_id][order](cate_feature[cate_id][order], f)
        if pre_query != "":
            for (cid, flist) in cate_feature.iteritems():
                out_str = pre_query
                for f in flist:
                    if f == "":
                        out_str += "\t-"
                    else:
                        out_str += "\t%s" % (f)
                print >> fout, "%s\t%s" % (out_str, cid)
        for cid in self.__custom_merge:
            result = self.__custom_merge[cid].merge_feature(None)
            if not result is None:
                result = [str(item) for item in result]
                print >> fout, "%s\t%s" % ("\t".join(result), cid)
        return True

#/* vim: set ts=4 sw=4 sts=4 tw=100 */
