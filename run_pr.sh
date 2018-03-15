hdfs dfs -rm -r -f out_pr && \
spark-submit --class spark.PageRank /home/2141380j/eclipse-workspace/spark/target/uog-bigdata-0.0.1-SNAPSHOT.jar wiki.txt out_pr 5 2008-01-01T00:00:00Z
