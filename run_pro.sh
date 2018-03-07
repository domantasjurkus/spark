hdfs dfs -rm -r -f out_pro && \
spark-submit --class spark.PageRankOriginal /home/2141380j/eclipse-workspace/spark/target/uog-bigdata-0.0.1-SNAPSHOT.jar wiki.txt out_pro
