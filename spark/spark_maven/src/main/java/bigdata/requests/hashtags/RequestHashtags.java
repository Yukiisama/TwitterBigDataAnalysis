package bigdata.requests.hashtags;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import bigdata.data.comparator.HashtagComparator;
import bigdata.data.parser.JsonUtils;
import scala.Tuple2;

// import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;
import static bigdata.TPSpark.files;
public class RequestHashtags {

	

	public static void mostUsedHashtags (int k) {
		if (k < 1 || k > 10000 ) {
			System.err.println("[ERROR] Invalid range in mostUsedHashtags@void, valid value is between 1 and 10000.");
			
			return;
		}
		
		// Premier essai sans construire tout le gson en une classe
		long startTime = System.currentTimeMillis();
		JavaRDD<String> hashtags = file.flatMap(line -> JsonUtils.withoutReflexivityAndWholeJson(line));
		JavaPairRDD<String, Integer> r = hashtags.mapToPair(hash -> new Tuple2<>(hash, 1)).reduceByKey((a, b) -> a + b);
		List<Tuple2<String, Integer>> top = r.top(k, new HashtagComparator());
		System.out.println(top);
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
	}
	

	public static void mostUsedHashtagsWithCount (int k) {
		if (k < 1 || k > 10000 ) {
			System.err.println("[ERROR] Invalid range in mostUsedHashtagsWithCount@void, valid value is between 1 and 10000.");
			
			return;
		}
		
		long startTime = System.currentTimeMillis();
		// on all files 
		JavaPairRDD<String, Integer> r = files.mapToPair(hash -> new Tuple2<>(hash, 1)).reduceByKey((a, b) -> a + b);
		List<Tuple2<String, Integer>> top = r.top(k, new HashtagComparator());
		System.out.println(top);
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
	}

	public static void numberOfApparitions (String hashtag_label) {
		// TODO

	}

	public static void usersList (String hashtag_label) {
		// TODO

	}
    
}
