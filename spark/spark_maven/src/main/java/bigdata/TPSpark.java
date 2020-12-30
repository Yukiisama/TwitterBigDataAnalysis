package bigdata;

import java.util.ArrayList;

import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.data.parser.JsonUtils;
import bigdata.requests.EntryPoint;

public class TPSpark {

	private static SparkConf conf = null;

	public static JavaRDD<String> file = null;
	public static JavaRDD<String> files = null;

	public static JavaSparkContext context = null;

	static {
		conf = new SparkConf()
				.setAppName("TP Spark")
				.set("spark.executor.instances", "5")
			    .set("spark.executor.cores", "2");

		context = new JavaSparkContext(conf);
		context.defaultParallelism();
		context.setLogLevel("ERROR");

		file = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
		files = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");


		// // int nb_of_workers = context.sc().getExecutorStorageStatus().length - 1;
		
		// System.out.println("There is " + context.sc().statusTracker().getExecutorInfos().length + " Workers.");

		// // file = context.textFile(JsonUtils.data[1]);
		// file = context.textFile(JsonUtils.data[5]);
		
		// String s = JsonUtils.data[1];

		// for (int i = 2; i < JsonUtils.data.length; i++) {
		// 	s = s.concat("," + JsonUtils.data[i]);
		// }
		// file = context.textFile(s);
		// file = context.textFile("/raw_data/tweet_05_03_2020.nljson");
	}

	
	public static void main (String[] args) {

		try {
			
			System.out.println("Number of partitions : " + file.getNumPartitions());
			System.out.println("Lines Count" + file.count());
			
			// Print val
			// file.foreach(f -> System.out.println(f));
			// hashtags.foreach( f -> System.out.println(f));
			// r.foreach(f -> System.out.println(f));


			AnalysisHashtags();
			AnalysisUser();

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			// Always close the Spark Context.
			context.close();
		}
	}


	/**
	 * Run Hashtag Analysis
	 */
	private static void AnalysisHashtags() {
		System.out.println("--- Hashtags ---");
		/**
		 * DONE
		 */
		System.out.println("a) K Hashtags les plus utilisés avec nombre d'apparition sur un jour:");
		EntryPoint.HASHTAGS_DAILY_TOPK.apply(10);
		System.out.println("b) K Hashtags les plus utilisés avec nombre d'apparition sur toutes les données: (commenté)");
		//EntryPoint.HASHTAGS_BEST_ALL_FILES_TOPK.apply(10);

		/**
		 * TODO
		 */
		System.out.println("c) Nombre d'apparitions d'un hashtag:");
		final Boolean allFiles = false; // Doit être fait sur tous les fichiers mais bon pour test.
		EntryPoint.HASHTAGS_APPARITIONS_COUNT.apply(allFiles);


		System.out.println("d) Utilisateurs ayant utilisé un Hashtag:");
		EntryPoint.HASHTAGS_USED_BY.apply();

		/**
		 * Optional
		 */
	}


	/**
	 * Run Users Analysis
	 */
	private static void AnalysisUser() {

		System.out.println("--- Utilisateurs ---");
		/**
		 * DONE
		 */
		// System.out.println("a) Liste des Hashtags sans doublons:");
		EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply();

		// /**
		//  * TODO
		//  */
		// System.out.println("c) Nombre de tweets par langue:");
		// EntryPoint.USERS_NUMBER_OF_TWEETS_PER_LANGAGE.apply("fr");
		// /**
		//  * Optional
		//  */
		// System.out.println("* Message le plus liké d'un utilisateur:");
		// // EntryPoint.USERS_MOST_LIKED_TWEET.apply("gouvernementFR");
		// System.out.println("* Message le plus retweeté d'un utilisateur:");
		// // EntryPoint.USERS_MOST_RETWEETED_TWEET.apply("LEXPRESS");
	}
}
