#!/bin/bash
cd `dirname $0`
source set_env.sh
#base_script_dir=$( cd ../; pwd)
#base_script_dir=$(cd "$(dirname "$0")"; cd ../; pwd)

#acquire job lock
job_name="mon_aladdin_update"
lock_path="${base_script_dir}/status/${job_name}.lock"


#lockfileµÄ¼ì²é£¬»»³É-e
#lockfile -r 0 ${lock_path}
if [[ -e $lock_path ]];then
	echo "fail to acquire lock: ${lock_path} status[$?], exit!"
	exit 1
fi
trap "rm -f ${lock_path}; exit $?" INT TERM EXIT


#echo $PYTHON_BIN ${base_script_dir}"/bin/gen_crawler_query.py" -i ${monitor_query_path} -o ${crawler_query_path}
$PYTHON_BIN ${base_script_dir}"/bin/gen_crawler_query.py" -i ${monitor_query_path} -o ${crawler_query_path}
if [ $? -ne 0 ]; then
	echo "${job_name} gen_crawler_query is abnormal!"
	exit 1
fi

cd ../../aladdin_crawler/bin  && bash run.sh -c ../conf/crawler.conf -q ${crawler_query_path} -o ${crawler_result_path}
if [ $? -ne 0 ]; then
	echo "${job_name} crawler is abnormal!"
	exit 1
fi
rm -f ${lock_path}
trap - INT TERM EXIT
