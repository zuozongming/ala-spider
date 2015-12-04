#!/bin/bash
#Author: duanjiawang
#���һ�㣬�����쳣�Ľ��������
PYTHON="/home/tools/tools/python/2.7.2/64/bin/python2.7"
error_email_list='duanjiawang@baidu.com'
pc_email_list='duanjiawang@baidu.com,zhangxiyuan@baidu.com,wangshuping03@baidu.com,zuozongming@baidu.com'
pad_email_list='duanjiawang@baidu.com'
monitor_query_path="../data/monitor_query.dict"
date

if [[ 1 == 1 ]]
then
	#����query�ļ�
	wget ftp://cq01-2011q4-setest3-17.vm.baidu.com:/home/work/all_video_off_line/tools/uri_pv_date_from_mergelog/top_500_query -O ${monitor_query_path}_temp

	cat ../conf/fix_query_trans.conf ../conf/doubt_query_trans.conf > ../conf/query_trans.conf


	$PYTHON monitor_query_check.py
	if [[ $? -ne 0 ]]; then echo "monitor_query_check error" | mail -s "��ERROR��Ӱ�Ӹ��¼�ر���" $error_email_list; exit ; fi

	#����ץȡ
	sh run.sh
	if [[ $? -ne 0 ]]; then echo "run yudong error" | mail -s "��ERROR��Ӱ�Ӹ��¼�ر���" $error_email_list; exit; fi

	#��������
	cat ../data/monitor.dict.readonly | $PYTHON  trouble_seeker.py
	if [[ $? -ne 0 ]]; then echo "trouble_seeker.py error" | mail -s "��ERROR��Ӱ�Ӹ��¼�ر���" $error_email_list; exit; fi
fi

error_query_num=`wc -l ../data/pc_monitor_out`
if [[ $error_query_num > 2 ]]
then
	echo query error, send mail
	cat ../data/pc_monitor_out | mail -s "Ӱ�Ӽ�ر���" $pc_email_list
fi
