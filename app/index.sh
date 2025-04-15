#!/bin/bash
# app/index.sh
echo "Starting indexing process with Hadoop MapReduce..."
echo "Input path: $1"

# Remove any previous temporary HDFS directory
hdfs dfs -rm -r -f /tmp/index

# Execute Hadoop streaming job. Adjust your paths to mapper and reducer accordingly.
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapred.reduce.tasks=3 \
  -input $1 \
  -output /tmp/index \
  -mapper "python /app/mapreduce/mapper1.py" \
  -reducer "python /app/mapreduce/reducer1.py"

# (Optional) Export final results from /tmp/index if you need to inspect the output.
hdfs dfs -cat /tmp/index/part-*
