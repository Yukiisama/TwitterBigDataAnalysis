package bigdata;

import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.data.parser.JsonUtils;
import bigdata.requests.EntryPoint;

public class TPSpark {

	private static SparkConf conf = null;
	public static JavaRDD<String> file = null;
	public static JavaSparkContext context = null;

	static {
		conf = new SparkConf()
				.setAppName("TP Spark")
				.set("spark.executor.instances", "20")
			    .set("spark.executor.cores", "2");
		context = new JavaSparkContext(conf);

		context.defaultParallelism();
		context.setLogLevel("ERROR");

		file = context.textFile(JsonUtils.data[1]);
		
		// file = context.textFile("/raw_data/tweet_02_03_2020.nljson");
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
		System.out.println("a) K Hashtags les plus utilisés avec nombre d'apparition:");
		EntryPoint.HASHTAGS_DAILY_TOPK.apply(10);
		System.out.println("b) K Hashtags les plus utilisés:");
		EntryPoint.HASHTAGS_BEST_WITH_COUNTS.apply(10);

		/**
		 * TODO
		 */
		System.out.println("c) Nombre d'apparitions d'un hashtag:");
		EntryPoint.HASHTAGS_USED_BY.apply("hashtag_text");


		System.out.println("d) Utilisateurs ayant utilisé un Hashtag:");
		EntryPoint.HASHTAGS_APPARITIONS_COUNT.apply("hashtag_text");

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
		System.out.println("a) Liste des Hashtags sans doublons:");
		EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply("jakefm");
		// EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply("LePoint");
		// EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply("gouvernementFR");
		// EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply("LEXPRESS");

		/**
		 * TODO
		 */
		System.out.println("b) Nombre de tweets d'un utilisateur:");
		// EntryPoint.USERS_NUMBER_OF_TWEETS.apply("gouvernementFR");
		System.out.println("c) Nombre de tweets par langue:");
		//EntryPoint.USERS_NUMBER_OF_TWEETS_PER_LANGAGE.apply("fr");
		/**
		 * Optional
		 */
		System.out.println("* Message le plus liké d'un utilisateur:");
		// EntryPoint.USERS_MOST_LIKED_TWEET.apply("gouvernementFR");
		System.out.println("* Message le plus retweeté d'un utilisateur:");
		// EntryPoint.USERS_MOST_RETWEETED_TWEET.apply("LEXPRESS");
	}
}
