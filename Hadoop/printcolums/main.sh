#!/bin/bash

# Current code directory
PWD=$(cd $(/home/devel/students/2020210977huojiaqi/ ); pwd)
cd $PWD 1> /dev/null 2>&1

# Asign a task name
TASKNAME=task2_jiaqi



# Hadoop input and output
HADOOP_INPUT_DIR=/user/devel/2020210977huojiaqi/README.txt
HADOOP_OUTPUT_DIR=/home/devel/students/2020210977huojiaqi/result/task2

echo $HADOOP_INPUT_DIR
echo $HADOOP_OUTPUT_DIR

hadoop fs -rmr $HADOOP_OUTPUT_DIR

hadoop   jar   $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar \
    -jobconf mapred.job.name=$TASKNAME \
    -jobconf mapred.job.priority=NORMAL \
    -jobconf mapred.map.tasks=500 \
    -jobconf mapred.reduce.tasks=500 \
    -jobconf mapred.job.map.capacity=500 \
    -jobconf mapred.job.reduce.capacity=500 \
    -jobconf stream.num.map.output.key.fields=2 \
    -jobconf mapred.text.key.partitioner.options=-k1,1 \
    -jobconf stream.memory.limit=1000 \
    -file /home/devel/students/2020210977huojiaqi/printcolums/mapper.sh \
    -output ${HADOOP_OUTPUT_DIR} \
    -input ${HADOOP_INPUT_DIR} \
    -mapper "sh mapper.sh" \
    -reducer "/usr/bin/cat" \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner


if [ $? -ne 0 ]; then
    echo 'error'
    exit 1
fi
hadoop fs -touchz ${HADOOP_OUTPUT_DIR}/done

exit 0