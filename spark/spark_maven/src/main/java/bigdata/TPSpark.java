package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.requests.EntryPoint;

public class TPSpark {

	private static SparkConf conf = null;
	public static JavaRDD<String> file = null;
	public static JavaSparkContext context = null;

	static {
		conf = new SparkConf().setAppName("TP Spark");
		context = new JavaSparkContext(conf);

		context.defaultParallelism();
		context.setLogLevel("ERROR");

		file = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
		// file = context.textFile("/raw_data/tweet_02_03_2020.nljson");
	}

	public static void main(String[] args) {
		try {
			// File Import (by args)
			// JavaRDD<String> file = context.textFile(JsonUtils.data[1]);


			System.out.println("Number of partitions : " + file.getNumPartitions());
			System.out.println("Lines Count" + file.count());
			// Print val
			// file.foreach(f -> System.out.println(f));
			// hashtags.foreach( f -> System.out.println(f));
			// r.foreach(f -> System.out.println(f));


			System.out.println("--- Hashtags ---");
			System.out.println("* K Hashtags les plus utilisés:");
			EntryPoint.HASHTAGS_DAILY_TOPK.apply(10);


			System.out.println("--- Utilisateurs ---");
			System.out.println("* Liste des Hashtags sans doublons:");
			EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply("jakefm");
			EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply("LePoint");
			EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply("gouvernementFR");
			EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply("LEXPRESS");

		} catch (Exception e) {

			e.printStackTrace();

		} finally {
			// Always close the Spark Context.
			context.close();
		}
	}

}
