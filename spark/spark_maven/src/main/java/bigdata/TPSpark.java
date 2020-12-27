package bigdata;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.comparator.HashtagComparator;
import bigdata.file.json.JsonUtils;
import scala.Tuple2;

public class TPSpark {

	private static JavaRDD<String> file = null;
	private static JavaSparkContext context = null;
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		context = new JavaSparkContext(conf);


		// file = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
		// file = context.textFile("/raw_data/tweet_02_03_2020.nljson");

		// JavaRDD<String> file = context.textFile(JsonUtils.data[1]);


		context.defaultParallelism();
		context.setLogLevel("ERROR");
		System.out.println("Number of partitions : " + file.getNumPartitions());
		System.out.println("Lines Count" + file.count());
		// Print val
		// file.foreach(f -> System.out.println(f));
		// hashtags.foreach( f -> System.out.println(f));
		// r.foreach(f -> System.out.println(f));

		mostUseHashtag(100, file);
	}
	
	public static void mostUseHashtag (int k, JavaRDD<String> file) {

		// mostUseHashtag(100);


		System.out.println("--- Utilisateurs ---");

		System.out.println("Liste des Hashtags sans doublons:");
		UserHashtagsList("jakefm");
		// UserHashtagsList("LePoint");
		UserHashtagsList("gouvernementFR");
		// UserHashtagsList("sadisaac_");
		// UserHashtagsList("LEXPRESS");
		// UserHashtagsList("RegexTip");



		context.close();
	}






	public static void mostUseHashtag (int k) {

		if (k < 1 || k > 10000 ) {
			System.err.println("[ERROR] Invalid range in mostUseHashtag@void, valid value is between 1 and 10000.");
			
			return;
		}

		// Premier essai sans construire tout le gson en une classe
		long startTime = System.currentTimeMillis();
		JavaRDD<String []> hashtags = file.flatMap(line -> JsonUtils.withoutReflexivityAndWholeJson(line));
		JavaPairRDD<String[], Integer> r = hashtags.mapToPair(hash -> new Tuple2<>(hash, 1)).reduceByKey((a, b) -> a + b);
		List<Tuple2<String[], Integer>> top = r.top(k, new HashtagComparator());
		for (Tuple2<String[], Integer> t: top) {
			System.out.println("hashtag: " + t._1[0] + " username: " + t._1[1] 
								+ " userId: " + t._1[2] + " count: " + t._2);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");

	}

	







	/*
	 * 
	 * 
	 * 
	 * Exemple d'analyse des users 
	 * 
	 * 
	 * 
	 */







	/**
	 * 
	 * @param k
	 * @param file
	 */

	public static void UserHashtagsList (String user_id) {
		// FAUX
		// // Input checking
		// Pattern p = Pattern.compile("\d{19}");
		// Matcher m = p.matcher(user_id);
		// if(!m.find()) {
		// 	System.err.println("[ERROR] Invalid input in mostUseHashtag@void, user_id must be a string matching regex r'\d{19}'.");
		// 	return;
		// }

		// Time calculation
		long startTime = System.currentTimeMillis();



		// Content


		// JavaRDD<String> hashtags = file.flatMap(line -> JsonUtils.withoutReflexivityAndWholeJson(line));
		// JavaPairRDD<String, Integer> r = hashtags.mapToPair(hash -> new Tuple2<>(hash, 1)).reduceByKey((a, b) -> a + b);
		// HashtagComparator comparator = new HashtagComparator();
		// List<Tuple2<String, Integer>> top = r.top(k, comparator);



		JavaPairRDD<String, HashSet<String>> user_hashtags = file.mapToPair(line 
				-> JsonUtils.withoutReflexivityAndWholeJsonUsers(user_id, line));
		
		if(user_hashtags == null) {
			System.err.println("[ERROR] Null dataset in UserHashtagsList@void, user probably don't have used any hashtags.");
		}

		Map<String, HashSet<String>> data = user_hashtags.collectAsMap();

		// Output
		System.out.println(data);
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
	}


}
