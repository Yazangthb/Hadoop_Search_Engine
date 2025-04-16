# #!/bin/bash
# # app/index.sh
# echo "Starting indexing process with Hadoop MapReduce..."
# echo "Input path: $1"

# hdfs dfs -rm -r -f /tmp/index

# hdfs dfs -mkdir -p /index/data/
# hdfs dfs -mkdir -p /tmp/

# #add the data file to hdfs
# hdfs dfs -put $1 /index/data/
# hdfs dfs -ls /index/data


# # hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
# # mapred streaming\
# #   -D mapred.reduce.tasks=3 \
# #   -files "/app/mapreduce/mapper1.py","/app/mapreduce/reducer2.py"\
# #   -archives "/app/.venv.tar.gz#.venv"\
# #   -input /index/$1 \
# #   -output /tmp/index \
# #   -mapper "python3 /app/mapreduce/mapper1.py" \
# #   -reducer ".venv/bin/python3 /app/mapreduce/reducer2.py"\
# #   && \
# #   hdfs dfs -cat /tmp/index/part-*

# mapred streaming\
#   -D mapred.reduce.tasks=3 \
#   -files "/app/mapreduce/mapper1.py","/app/mapreduce/reducer1.py"\
#   -archives "/app/.venv.tar.gz#.venv"\
#   -input "/index/460442_A_Bug's_Life.txt" \
#   -output /tmp/index6 \
#   -mapper ".venv/bin/python3 mapper1.py" \
#   -reducer ".venv/bin/python3 reducer1.py"\
#   && \
#   hdfs dfs -cat /tmp/index/part-*




#!/bin/bash
# app/index.sh

# echo "Starting indexing process with Hadoop MapReduce..."
# echo "Input file path on host: $1"

# filename=$(basename "$1")

# # Clean previous output (optional)
# hdfs dfs -rm -r -f /tmp/index_output

# # Ensure HDFS directories exist
# hdfs dfs -mkdir -p /index/data/
# hdfs dfs -mkdir -p /tmp/

# # Upload the file to HDFS
# hdfs dfs -put -f "$1" "/index/data/$filename"
# hdfs dfs -ls /index/data/

# # Run Hadoop Streaming with virtual environment
# mapred streaming \
#   -D mapred.reduce.tasks=3 \
#   -files "/app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py" \
#   -archives "/app/.venv.tar.gz#.venv" \
#   -input "/index/data/$filename" \
#   -output "/tmp/index_output" \
#   -mapper ".venv/bin/python3 mapper1.py" \
#   -reducer ".venv/bin/python3 reducer1.py" \
#   && \
#   echo "MapReduce job finished. Output:" && \
#   hdfs dfs -cat /tmp/index_output/part-*


#!/bin/bash
# app/index.sh

echo "Starting indexing process with Hadoop MapReduce..."
echo "Input file path on host: $1"

filename=$(basename "$1")

# Clean previous output (optional)
hdfs dfs -rm -r -f /tmp/index_output

# Ensure HDFS directories exist
hdfs dfs -mkdir -p /index/data/
hdfs dfs -mkdir -p /tmp/

# Upload the file to HDFS
hdfs dfs -put -f "$1" "/index/data/$filename"
hdfs dfs -ls /index/data/

# Run Hadoop Streaming with virtual environment
mapred streaming \
  -D mapred.reduce.tasks=3 \
  -files "/app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py" \
  -archives "/app/.venv.tar.gz#.venv" \
  -input "/index/data/$filename" \
  -output "/tmp/index_output" \
  -mapper ".venv/bin/python3 mapper1.py" \
  -reducer ".venv/bin/python3 reducer1.py" \
  && \
  echo "MapReduce job finished for $filename. Output:" && \
  hdfs dfs -cat /tmp/index_output/part-*
