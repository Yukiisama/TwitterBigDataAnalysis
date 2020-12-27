package bigdata;

import bigdata.file.json.JsonUtils;
import bigdata.comparator.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import scala.Tuple2;

public class TPSpark {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> file = context.textFile(JsonUtils.data[1]);
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
