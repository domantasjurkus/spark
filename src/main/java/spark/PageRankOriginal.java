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

import com.google.common.base.Optional;
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
			Set<String> outs = new HashSet<>();
			
			// If MAIN is not empty (no outlinks)
			if (!lines[3].equals("MAIN")) {
				String[] mainLine = lines[3].substring(5, lines[3].length()).split("\\s+");
				outs.addAll(Arrays.asList(mainLine));
			}

			// Remove self reference
			if (outs.contains(articleTitle)) outs.remove(articleTitle);
			
			return Collections.singleton(new Tuple2<String, Iterable<String>>(articleTitle, outs));
		});
		
		//outlinks.saveAsTextFile(args[1]);
		
		// Give revisions and outlinks initial ranks
		JavaPairRDD<String, Double> ranks = outlinks.flatMapToPair(tuple -> {
			List<Tuple2<String, Double>> ret = new ArrayList<Tuple2<String, Double>>();
			for (String out : tuple._2) {
				ret.add(new Tuple2<String, Double>(out, 1.0));
			}
			ret.add(new Tuple2<String, Double>(tuple._1, 1.0));
			return ret;
		}).distinct();		
		
		//JavaPairRDD<String, Tuple2<Optional<Double>, Optional<Iterable<String>>>> ranksAndLinks = ranks.fullOuterJoin(outlinks);
		//ranksAndLinks.saveAsTextFile(args[1]);
		
		// Update contributions to outlinks
		int iterations = 5;
		for (int i=0; i<iterations; i++) {
			JavaPairRDD<String, Tuple2<Optional<Double>, Optional<Iterable<String>>>> ranksAndLinks = ranks.fullOuterJoin(outlinks);
			
			JavaPairRDD<String, Double> contribs = ranksAndLinks.flatMapToPair(tuple -> {
				List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
				
				String articleTitle = tuple._1();
				Double currentRank = tuple._2._1().get();

				// Release 0.0 for article name
				res.add(new Tuple2<String, Double>(articleTitle, 0.0));
				
				// If article has outlinks
				if (tuple._2._2.isPresent()) {
					int outlinkCount = Iterables.size(tuple._2._2.get());
					for (String out : tuple._2._2.get()) {
						res.add(new Tuple2<String, Double>(out, currentRank/outlinkCount));
					}
				}
				
				return res;
			});

			ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v*0.85);
		}
		
		// Sort
		ranks = ranks.mapToPair(tuple -> new Tuple2<Double, String>(tuple._2, tuple._1))
				.sortByKey(false)
				.mapToPair(tuple -> new Tuple2<String, Double>(tuple._2, tuple._1));
		
		ranks.saveAsTextFile(args[1]);
		w.close();
		sc.close();
	}
}