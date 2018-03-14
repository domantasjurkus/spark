package spark;

import java.util.List;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.google.common.collect.Iterables;
import scala.Tuple2;
import utils.ISO8601;

public class PageRankOriginal {
	
	/** Transform ISO 8601 string to timestamp. */
	public static long toTimeMS(final String iso8601string)
			throws ParseException {
		String s = iso8601string.replace("Z", "+00:00");
		try {
			s = s.substring(0, 22) + s.substring(23); // to get rid of the ":"
		} catch (IndexOutOfBoundsException e) {
			throw new ParseException("Invalid length", 0);
		}
		Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s);

		return date.getTime();
	}
	
	public static String getArticleTitle(String revision) {
		return revision.split("\\s+")[3];
	}
	
	public static <T> void debug(T s) {		
		try {	
			PrintWriter writer = new PrintWriter("debug.txt", "UTF-8");
			writer.println(s);
			writer.close();
		} catch (Exception e) {}
	}

	//
	// NOTE: double-check on cluster if any timestamp parsing exceptions are thrown
	//
	public static void main(String[] args) throws ParseException {
		SparkConf conf = new SparkConf();
		conf.setAppName("PageRankOriginal");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n");
		JavaRDD<String> revisions = sc.textFile(args[0], 1);
		
		String Date = "2010-01-01T00:00:00Z";
		long inputDate = toTimeMS(Date);
		
		// 39129 revisions
		// 35266 unique articles
		// ~700 revisions with empty MAIN
		
		//Accumulator<Integer> acc = sc.accumulator(0);
		//acc.add(1);

		// Change this to flatMapToPair so that you can filter out future revisions earlier
		//revisions = revisions.mapToPair(rev -> {
		revisions = revisions.flatMapToPair(rev -> {
			
			// if date in future, return Collections.emptyList();
			long date = toTimeMS(rev.split("\\s+")[4]);
			if (date > inputDate) {
				return Collections.emptyList();
			}
			
			return Collections.singleton(new Tuple2<String, String>(getArticleTitle(rev), rev));
		}).reduceByKey((rev1, rev2) -> {
			// Find the most relevant revision based on abs(time_difference);
			long date1 = toTimeMS(rev1.split("\\s+")[4]);
			long date2 = toTimeMS(rev2.split("\\s+")[4]);
			
			if (date1 > date2) {
				return rev1;
			}
			
			/*if (Math.abs(date1-inputDate) < Math.abs(date2-inputDate)) {
				return rev1;
			}*/
			return rev2;
		})
		
		/*.filter(titleAndRevisionTuple -> {
			// Filter out revisions that are in the future
			String revisionDate = titleAndRevisionTuple._2.split("\\s+")[4];
			long revisionTimestamp = toTimeMS(revisionDate);
			if (revisionTimestamp > inputDate)
				return false;
			return true;
		})*/
		.map(tuple -> tuple._2);
		
		debug(revisions.count());
		
		// Make an RDD of [(articleTitle, [out, out, out]), ...]
		/*JavaPairRDD<String, Iterable<String>> outlinks = revisions.mapToPair(revision -> {
			String[] lines = revision.split("\n");
			String articleTitle = lines[0].split("\\s+")[3];
			
			String[] mainLine = {};
			
			// If MAIN is not empty
			if (!lines[3].equals("MAIN")) {
				mainLine = lines[3].substring(5, lines[3].length()).split("\\s+");
			}
			
			return new Tuple2<String, Iterable<String>>(articleTitle, Arrays.asList(mainLine));
		}).distinct();//.groupByKey();
		
		// Give articles initial score
		// [(articleTitle, 1.0), ...]
		JavaPairRDD<String, Double> ranks = outlinks.mapValues(s -> 1.0);
		
		int iterations = 1;
		for (int i=0; i<iterations; i++) {
			JavaPairRDD<String, Double> contribs = outlinks.join(ranks).values().flatMapToPair(v -> {
				List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
				int urlCount = Iterables.size(v._1);
				for (String s : v._1)
					res.add(new Tuple2<String, Double>(s, v._2() / urlCount));
				return res;
			});
			ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v*0.85);
		}*/
		
		//List<Tuple2<String, Double>> output = ranks.collect();
		
		//ranks.saveAsTextFile(args[1]);
		revisions.saveAsTextFile(args[1]);
		
		sc.close();
	}
}