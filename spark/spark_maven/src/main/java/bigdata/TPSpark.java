package bigdata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


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
		//file.foreach(f -> System.out.println(f));
		
		//Premier essai sans construire tout le gson en une classe
		long startTime = System.currentTimeMillis();
		JavaRDD<List<String>> v =  file.map(line -> {
			return withoutReflexivityAndWholeJson(line);
		});
		v.foreach( f -> System.out.println(f));
		
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map) " + (endTime - startTime) + " milliseconds");
		
		
	}
	
		public static List<String> withoutReflexivityAndWholeJson(String line) {
			List<String> hashs = new ArrayList<>();
			JsonElement json = new JsonParser().parse(line);
			JsonObject jsonObj =  json.getAsJsonObject();
			JsonElement entities = jsonObj.get("entities");
			if (entities != null && entities.isJsonObject()) {
				JsonElement hashtags = (entities.getAsJsonObject()).get("hashtags");
				if (hashtags != null) {
					for (JsonElement hash : hashtags.getAsJsonArray()) {
						hashs.add(hash.getAsJsonObject().get("text").getAsString());
					}
					return hashs;
				}
			}
			return hashs;
		}
		
	
}
