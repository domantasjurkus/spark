package spark;

import java.util.List;
import java.util.Set;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.google.common.collect.Iterables;
import scala.Tuple2;

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
	
	public static String getRevisionDate(String revision) {
		return revision.split("\\s+")[4];
	}
	
	//
	// NOTE: double-check on cluster if any timestamp parsing exceptions are thrown
	//
	public static void main(String[] args) throws ParseException {
		SparkConf conf = new SparkConf();
		conf.setAppName("PageRankOriginal");
		
		PrintWriter w = null;
		try {w = new PrintWriter("debug.txt", "UTF-8");}
		catch (Exception e) {}
		
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

		// Get rid of future revisions
		revisions = revisions.flatMapToPair(rev -> {
			long date = toTimeMS(getRevisionDate(rev));
			if (date > inputDate) {
				return Collections.emptyList();
			}
			
			// We need to do this due to requirement to return immutable object
			return Collections.singleton(new Tuple2<String, String>(getArticleTitle(rev), rev));
		// Get rid of past revisions
		}).reduceByKey((rev1, rev2) -> {
			long date1 = toTimeMS(getRevisionDate(rev1));
			long date2 = toTimeMS(getRevisionDate(rev2));
			
			return (date1 > date2) ? rev1 : rev2;
		}).map(tuple -> tuple._2);
		
		JavaPairRDD<String, Iterable<String>> outlinks = revisions.flatMapToPair(revision -> {
			String[] lines = revision.split("\n");
			String articleTitle = getArticleTitle(lines[0]);
			
			// If MAIN is empty (no outlinks)
			if (lines[3].equals("MAIN")) {
				return Collections.emptyList();
			}
			String[] mainLine = lines[3].substring(5, lines[3].length()).split("\\s+");
			
			// Get rid of dupes and self
			Set<String> outs = new HashSet<>(Arrays.asList(mainLine));
			if (outs.contains(articleTitle)) outs.remove(articleTitle);
			
			if (outs.isEmpty()) return Collections.emptyList();
			
			return Collections.singleton(new Tuple2<String, Iterable<String>>(articleTitle, outs));
		});
		
		// Give articles initial ranks
		JavaPairRDD<String, Double> articleRanks = revisions.mapToPair(revision -> {
			return new Tuple2<String, Double>(getArticleTitle(revision), 1.0);
		});
		
		// Give outlinks initial ranks
		JavaPairRDD<String, Double> ranks = outlinks.flatMapToPair(tuple -> {
			List<Tuple2<String, Double>> ret = new ArrayList<Tuple2<String, Double>>();
			for (String out : tuple._2) {
				ret.add(new Tuple2<String, Double>(out, 1.0));
			}
			return ret;
		}).union(articleRanks);
		
		ranks.saveAsTextFile(args[1]);
		
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
		}
		
		// Add tuples with inlinkless articles with a score of 0.15
		// TODO
		
		//List<Tuple2<String, Double>> output = ranks.collect();
		
		w.close();
		sc.close();
	}
}