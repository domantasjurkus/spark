package spark;

import java.util.List;
import java.io.PrintWriter;
import java.util.ArrayList;

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
		
		// Debug
		final Accumulator<Integer> count = sc.accumulator(0);
		
		// Find article name, revision id and MAIN
		revisions.foreach(r -> {
			String lines[] = r.split("\n");
			
			for (String line : lines) {
				String symbols[] = line.split("\\s+");
				if (symbols[0].equals("MAIN")) {
					count.add(1);
				}
			}
		});
		
		try {
			PrintWriter writer = new PrintWriter("debug.txt", "UTF-8");
			writer.println(count);
			writer.close();
		} catch (Exception e) {}
		
		
		// Assuming we're on line MAIN
		JavaPairRDD<String, Iterable<String>> links = revisions.mapToPair(s -> {
			String[] parts = s.split("\\s+");
			return new Tuple2<String, String>(parts[0], parts[1]);
		}).distinct().groupByKey().cache();
		
		JavaPairRDD<String, Double> ranks = links.mapValues(s -> 1.0);
		
		//int numIterations = Integer.parseInt(args[2]);
		int numIterations = 1;
		/*for (int current=0; current<numIterations; current++) {
			JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(v -> {
				List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
				int urlCount = Iterables.size(v._1);
				for (String s : v._1)
					res.add(new Tuple2<String, Double>(s, v._2() / urlCount));
				return res;
			});
			ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v * 0.85);
		}
		
		List<Tuple2<String, Double>> output = ranks.collect();
		
		ranks.saveAsTextFile(args[1]);*/
	}
}