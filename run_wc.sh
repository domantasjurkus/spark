hdfs dfs -rm -r -f wc_out && \
spark-submit --class spark.WordCount /home/2141380j/eclipse-workspace/spark/target/uog-bigdata-0.0.1-SNAPSHOT.jar wiki.txt wc_out
