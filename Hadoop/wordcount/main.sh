#!/bin/bash

PWD=$(cd $(/home/devel/students/2020210977huojiaqi/); pwd)
cd $PWD 1> /dev/null 2>&1

TASKNAME=task7_jiaqi
# python location on hadoop
PY27='/fli/tools/python2.7.tar.gz'
# hadoop client

HADOOP_INPUT_DIR1=/user/devel/2020210977huojiaqi/README.txt
HADOOP_OUTPUT_DIR=/home/devel/students/2020210977huojiaqi/result/task7

echo $HADOOP_HOME
echo $HADOOP_INPUT_DIR
echo $HADOOP_OUTPUT_DIR

hadoop fs -rmr $HADOOP_OUTPUT_DIR

hadoop   jar   $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar  \
    -jobconf mapred.job.name=$TASKNAME \
    -jobconf mapred.job.priority=NORMAL \
    -jobconf mapred.map.tasks=100 \
    -jobconf mapred.reduce.tasks=10 \
    -jobconf mapred.job.map.capacity=100 \
    -jobconf mapred.job.reduce.capacity=100 \
    -jobconf stream.num.map.output.key.fields=1 \
    -jobconf mapred.text.key.partitioner.options=-k1,1 \
    -jobconf stream.memory.limit=1000 \
    -file  /home/devel/students/2020210977huojiaqi/example/wordcount/reducer.py \
    -output ${HADOOP_OUTPUT_DIR} \
    -input ${HADOOP_INPUT_DIR1} \
    -mapper "/usr/bin/cat" \
    -reducer "python reducer.py" \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner


if [ $? -ne 0 ]; then
    echo 'error'
    exit 1
fi
$HADOOP_HOME fs -touchz ${HADOOP_OUTPUT_DIR}/done

exit 0