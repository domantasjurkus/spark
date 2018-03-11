package spark;

import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount"));
		
		// Load
		JavaRDD<String> logData = sc.textFile(args[0]);
		
		// Split
		JavaRDD<String> words = logData.flatMap(line -> {
			return Arrays.asList(line.split(" "));
		});
		
		// Map word to (word,1) and reduce by key
		JavaPairRDD<String, Integer> counts = words.mapToPair(word ->
			new Tuple2<String, Integer>(word, 1)
		).reduceByKey((a,b) -> a+b);
		
		// Debug
		/*try {
			PrintWriter writer = new PrintWriter("debug.txt", "UTF-8");
			writer.println(words.count());
			writer.close();
		} catch (Exception e) {}*/
		
		counts.saveAsTextFile(args[1]);
		sc.close();
	}
}
