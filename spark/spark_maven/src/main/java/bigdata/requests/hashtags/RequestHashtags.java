package bigdata.requests.hashtags;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import bigdata.data.User;
import bigdata.data.comparator.HashtagComparator;
import bigdata.data.parser.JsonUserReader;
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
        JavaRDD<String> hashtags = file.flatMap(line -> JsonUtils.getHashtagFromJson(line));
        JavaPairRDD<String, Integer> r = hashtags.mapToPair(hash -> new Tuple2<>(hash, 1)).reduceByKey((a, b) -> a + b);
        List<Tuple2<String, Integer>> top = r.top(k, new HashtagComparator());
        System.out.println(top);
        long endTime = System.currentTimeMillis();
        System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
    }
    

    public static void mostUsedHashtagsOnAllFiles(int k) {
        if (k < 1 || k > 10000 ) {
            System.err.println("[ERROR] Invalid range in mostUsedHashtagsWithCount@void, valid value is between 1 and 10000.");
            
            return;
        }

        long startTime = System.currentTimeMillis();
        JavaPairRDD<String, Integer> unionFiles = files
        										   .get(0)
                                                   .flatMap(line -> JsonUtils.getHashtagFromJson(line))
                                                   .mapToPair(hash -> new Tuple2<>(hash, 1))
                                                   .reduceByKey((a, b) -> a + b);
  
        for (int i = 1; i < files.size(); i++) {
            unionFiles = unionFiles.union(
            		files
            		.get(i)
                    .flatMap(line -> JsonUtils.getHashtagFromJson(line))
                    .mapToPair(hash -> new Tuple2<>(hash, 1))
                    .reduceByKey((a, b) -> a + b)
            		);
        }
        List<Tuple2<String, Integer>> top = unionFiles
                                            .reduceByKey((a, b) -> a + b)
                                            .top(k, new HashtagComparator());
        System.out.println(top);
        // On finit le travail
        unionFiles.unpersist(); // pas sur que ça ait un effet mais on sait jamais
        files.clear();
        long endTime = System.currentTimeMillis();
        System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
    }

    public static void numberOfApparitions (boolean allFiles) {
        
        long startTime = System.currentTimeMillis();
        JavaPairRDD<String, Integer> res = 	file	//((allFiles) ? files : file)
                                            .flatMap(line -> JsonUtils.getHashtagFromJson(line))
                                            .mapToPair(hash -> new Tuple2<>(hash, 1))
                                            .reduceByKey((a, b) -> a + b);
        
        //System.out.println(res.collectAsMap()); // A enlever après fais exploser la mémoire
        res.take(10).forEach(f -> System.out.println(f));
        long endTime = System.currentTimeMillis();
        System.out.println("That took without Reflexivity : (map + reduce) " + (endTime - startTime) + " milliseconds");

    }

    public static void usersList (boolean allFiles) {
        	
    	long startTime = System.currentTimeMillis();
    	
    	JavaPairRDD<String, User> users = file//((allFiles) ? files : file)
    			                          .mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
    			                          .filter( val ->  val._1() != "")
    			                          .filter( val -> val._2()._hashtags().size() > 0)
    									  .reduceByKey((user1, user2 ) -> user1);
    	
    	// Simplement enregister le set de clés (correspondants aux usernames) dans hbase 
    	// i.e  users.keys()
    	//System.out.println(users.collectAsMap()); // A enlever après fais exploser la mémoire
        users.take(10).forEach(f -> System.out.println(f));
    
        long endTime = System.currentTimeMillis();
        System.out.println("That took without Reflexivity : (map + reduce ) " + (endTime - startTime) + " milliseconds");
    	
        
    }
    
}
