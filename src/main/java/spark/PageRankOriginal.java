package spark;

import java.util.List;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.google.common.collect.Iterables;
import scala.Tuple2;

public class PageRankOriginal {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("PageRankOriginal");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n");
		JavaRDD<String> revisions = sc.textFile(args[0], 1);
		
		// 39129 revisions
		// 35266 unique articles
		// ~700 revisions with empty MAIN
		
		//Accumulator<Integer> acc = sc.accumulator(0);
		//acc.add(1);
		
		// Make an RDD of <articleTitle, Iterable<outlink>>
		JavaPairRDD<String, Iterable<String>> outlinks = revisions.mapToPair(revision -> {
			String[] lines = revision.split("\n");
			String articleTitle = lines[0].split("\\s+")[3];
			
			String[] mainLine = {};
			
			// If MAIN is not empty
			if (!lines[3].equals("MAIN")) {
				mainLine = lines[3].substring(5, lines[3].length()).split("\\s+");
			}
			
			return new Tuple2<String, Iterable<String>>(articleTitle, Arrays.asList(mainLine));
		}).distinct();//.groupByKey();ll
		
		// Debug
		/*try {
			PrintWriter writer = new PrintWriter("debug.txt", "UTF-8");
			writer.println(acc.value());
			writer.close();
		} catch (Exception e) {}*/
		
		// Give articles initial score
		JavaPairRDD<String, Double> ranks = outlinks.mapValues(s -> 1.0);
		
		int numIterations = 1;
		for (int current=0; current<numIterations; current++) {
			JavaPairRDD<String, Double> contribs = outlinks.join(ranks).values().flatMapToPair(v -> {
				List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
				int urlCount = Iterables.size(v._1);
				for (String s : v._1)
					res.add(new Tuple2<String, Double>(s, v._2() / urlCount));
				return res;
			});
			ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v*0.85);
		}
		
		//List<Tuple2<String, Double>> output = ranks.collect();
		
		ranks.saveAsTextFile(args[1]);
		
		sc.close();
	}
}