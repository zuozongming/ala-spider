#!/bin/bash
#脚本只留有用的，或者根本就不该从别的脚本改


base_script_dir=$(cd "$(dirname "$0")"; cd ../; pwd)

#acquire job lock
job_name="mon_aladdin_update"
lock_path="${base_script_dir}/status/${job_name}.lock"


#lockfile的检查，换成-e
#lockfile -r 0 ${lock_path}
if [[ -e $lock_path ]];then
#if [ $? -ne 0 ]; then
	echo "fail to acquire lock: ${lock_path} status[$?], exit!"
	exit 1
fi
#make sure the lockfile is removed when we exit and then claim it
#这些都知道是啥意思吗
trap "rm -f ${lock_path}; exit $?" INT TERM EXIT

PYTHON_BIN="/home/tools/tools/python/2.7.2/64/bin/python"
PYTHON_BIN="/home/tools/tools/python/2.7.2/64/bin/python2.7"
crawler_query_path="${base_script_dir}/data/crawler_query.dict"
crawler_result_path="${base_script_dir}/data/crawler_result.dict"
monitor_query_path="${base_script_dir}/data/monitor_query.dict"
$PYTHON_BIN ${base_script_dir}"/bin/gen_crawler_query.py" -i ${monitor_query_path} -o ${crawler_query_path}
if [ $? -ne 0 ]; then
	echo "${job_name} gen_crawler_query is abnormal!"
	exit 1
fi

#cd /home/work/aladdin_crawler/bin && bash run.sh -c ../conf/crawler.conf -q ${crawler_query_path} -o ${crawler_result_path}
#cd /home/work/duanjiawang/project/120all_video_off_line/aladdin_crawler/bin && bash run.sh -c ../conf/crawler.conf -q ${crawler_query_path} -o ${crawler_result_path}
cd /home/users/ligang01/duanjiawang/zuozongming/project/003spider_monitor/aladdin_crawler/bin  && bash run.sh -c ../conf/crawler.conf -q ${crawler_query_path} -o ${crawler_result_path}
if [ $? -ne 0 ]; then
	echo "${job_name} crawler is abnormal!"
	exit 1
fi

#这些东西不要就扔掉
#monitor_result_path="${base_script_dir}/data/monitor.dict"
#$PYTHON_BIN ${base_script_dir}"/bin/save_crawler_result.py" -r ${crawler_result_path} -s ${monitor_result_path}
if [ $? -ne 0 ]; then
	echo "${job_name} save crawler result is abnormal!"
	exit 1
fi

#monitor_result_path都被你注释掉了
#cp ${monitor_result_path} ${monitor_result_path}".readonly"

#release job lock, and release trap
rm -f ${lock_path}
trap - INT TERM EXIT
