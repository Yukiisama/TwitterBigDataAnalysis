package bigdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class TPSpark {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> file = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
		context.defaultParallelism();
		context.setLogLevel("ERROR");
		System.out.println("Number of partitions : " + file.getNumPartitions());
		System.out.println("Lines Count" + file.count());
		// Print val
		// file.foreach(f -> System.out.println(f));
		// hashtags.foreach( f -> System.out.println(f));
		// r.foreach(f -> System.out.println(f));
		// Premier essai sans construire tout le gson en une classe
		long startTime = System.currentTimeMillis();
		JavaRDD<String> hashtags = file.flatMap(line -> withoutReflexivityAndWholeJson(line));
		JavaPairRDD<String, Integer> r = hashtags.mapToPair(hash -> new Tuple2<>(hash, 1)).reduceByKey((a, b) -> a + b);
		TupleHashComparator comparator = new TupleHashComparator();
		List<Tuple2<String, Integer>> top = r.top(100, comparator);
		System.out.println(top);
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");

	}
	
	static private class TupleHashComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
		@Override
		public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
			return Integer.compare(t1._2, t2._2);
		}
		
	}
	
	public static Iterator<String> withoutReflexivityAndWholeJson(String line) {
		List<String> hashs = new ArrayList<>();
		JsonElement json = new JsonParser().parse(line);
		JsonObject jsonObj = json.getAsJsonObject();
		JsonElement entities = jsonObj.get("entities");
		if (entities != null && entities.isJsonObject()) {
			JsonElement hashtags = (entities.getAsJsonObject()).get("hashtags");
			if (hashtags != null) {
				for (JsonElement hash : hashtags.getAsJsonArray()) {
					hashs.add(hash.getAsJsonObject().get("text").getAsString());
				}
				return hashs.iterator();
			}
		}
		return hashs.iterator();
	}

}
