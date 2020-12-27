package bigdata.requests.hashtags;


import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import bigdata.data.comparator.HashtagComparator;
import bigdata.data.parser.JsonUtils;
import scala.Tuple2;

// import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;

public class RequestHashtags {
	public static void mostUseHashtag (int k) {
		if (k < 1 || k > 10000 ) {
			System.err.println("[ERROR] Invalid range in mostUseHashtag@void, valid value is between 1 and 10000.");
			
			return;
		}

		// Premier essai sans construire tout le gson en une classe
		long startTime = System.currentTimeMillis();
		JavaRDD<String> hashtags = file.flatMap(line -> JsonUtils.withoutReflexivityAndWholeJson(line));
		JavaPairRDD<String, Integer> r = hashtags.mapToPair(hash -> new Tuple2<>(hash, 1)).reduceByKey((a, b) -> a + b);
		HashtagComparator comparator = new HashtagComparator();
		List<Tuple2<String, Integer>> top = r.top(k, comparator);
		System.out.println(top);
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");

    }
    
}
