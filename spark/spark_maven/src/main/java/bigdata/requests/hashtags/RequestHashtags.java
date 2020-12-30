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
        List<Tuple2<String, Integer>> top = files
                                            .flatMap(line -> JsonUtils.getHashtagFromJson(line))
                                            .mapToPair(hash -> new Tuple2<>(hash, 1))
                                            .reduceByKey((a, b) -> a + b)
                                            .top(k, new HashtagComparator());
        System.out.println(top);
        long endTime = System.currentTimeMillis();
        System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
    }

    public static void numberOfApparitions (boolean allFiles) {
        
        long startTime = System.currentTimeMillis();
        JavaPairRDD<String, Integer> res = ((allFiles) ? files : file)
                                            .flatMap(line -> JsonUtils.getHashtagFromJson(line))
                                            .mapToPair(hash -> new Tuple2<>(hash, 1))
                                            .reduceByKey((a, b) -> a + b);
        
        System.out.println(res.collectAsMap()); // A enlever après fais exploser la mémoire
        
        long endTime = System.currentTimeMillis();
        System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");

    }

    public static void usersList () {
        // TODO
        
    }
    
}
