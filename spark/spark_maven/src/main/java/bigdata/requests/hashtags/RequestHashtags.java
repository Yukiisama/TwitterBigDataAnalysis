package bigdata.requests.hashtags;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.hadoop.hbase.KeyValue;

import static bigdata.TPSpark.logger;

import bigdata.data.User;
import bigdata.data.comparator.HashtagComparator;
import bigdata.data.parser.JsonUserReader;
import bigdata.data.parser.JsonUtils;
import bigdata.infrastructure.database.runners.HBaseTopKHashtag;
import bigdata.infrastructure.database.runners.HBaseUserHashtag;
import scala.Tuple2;

// import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;
import static bigdata.TPSpark.files;
import static bigdata.TPSpark.openFiles;
public class RequestHashtags {

    public static HBaseTopKHashtag hbaseTopk = new HBaseTopKHashtag("topKHashtag");
    public static HBaseTopKHashtag hbaseTopkall = new HBaseTopKHashtag("topKHashtagAll");
    public static HBaseTopKHashtag hbaseHashtags = new HBaseTopKHashtag("Hashtags");
    public static HBaseUserHashtag hbaseUserHashtags = HBaseUserHashtag.INSTANCE();
    /*public static HBaseTopKHashtag hbaseTopk = null;
    public static HBaseTopKHashtag hbaseTopkall = null;
    public static HBaseTopKHashtag hbaseHashtags = null;
    public static HBaseUserHashtag hbaseUserHashtags = null;*/

    
    public static void activateHbase(boolean activate) {
    	hbaseTopk = (activate) ? new HBaseTopKHashtag("topKHashtag") : null;
        hbaseTopkall = (activate) ? new HBaseTopKHashtag("topKHashtagAll") : null;
        hbaseHashtags = (activate) ? new HBaseTopKHashtag("Hashtags") : null;
        hbaseUserHashtags = (activate) ? HBaseUserHashtag.INSTANCE() : null;
    }
	
    //public static HBaseTopKHashtag hbaseHashtagsDay = new HBaseTopKHashtag("HashtagsDay");

    public static void nextDayHbase(int i){
        hbaseTopk = new HBaseTopKHashtag("topKHashtag" + i);
    }

    public static void mostUsedHashtags (int k) {
        if (k < 1 || k > 10000 ) {
            logger.fatal("Invalid range in mostUsedHashtags@void, valid value is between 1 and 10000.");
            
            return;
        }

        long startTime = System.currentTimeMillis();
        JavaRDD<String> hashtags = file.flatMap(line -> JsonUtils.getHashtagFromJson(line));
        JavaPairRDD<String, Integer> r = hashtags.mapToPair(hash -> new Tuple2<>(hash, 1)).reduceByKey((a, b) -> a + b);   

        // Write all hashtags of the day with their count
        //r.foreach(tuple -> hbaseHashtagsDay.writeTable(tuple));
        //hbaseHashtagsDay.resetPos();
        
        
        List<Tuple2<String, Integer>> top = r.top(k, new HashtagComparator());
        //logger.debug(top);

        // Write top to Hbase
        top.forEach(tuple -> hbaseTopk.writeTable(tuple));
        hbaseTopk.resetPos();
        long endTime = System.currentTimeMillis();

        // Clear memory
        r.unpersist();

        logger.info("Request Hashtag: That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds"); 
    }

    public static void mostUsedHashtagsOnAllFiles(int k) {
        if (k < 1 || k > 10000 ) {
            logger.fatal("Invalid range in mostUsedHashtagsWithCount@void, valid value is between 1 and 10000.");
            
            return;
        }
        openFiles();

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
                    ).unpersist();
        }

        unionFiles = unionFiles.reduceByKey((a, b) -> a + b);
        //logger.info("count union" + unionFiles.count());

        List<Tuple2<String, Integer>> top = unionFiles
                                            .top(k, new HashtagComparator());
        logger.debug(top);
       
        // Write top to Hbase for all days
        top.forEach(tuple -> hbaseTopkall.writeTable(tuple));
        hbaseTopkall.resetPos();
        long endTime = System.currentTimeMillis();
        logger.info("topkALL Hashtags: That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
        logger.debug("c) Nombre d'apparitions d'un hashtag:");
        // unionFiles.take(10).forEach(f -> System.out.println(f));

        // Write all hashtags of the day with their count
        unionFiles.foreach(tuple -> hbaseHashtags.writeTable(tuple));
        hbaseHashtags.resetPos();
        
        // Clear memory
        unionFiles.unpersist(); 
        files.clear();
        endTime = System.currentTimeMillis();
        
		logger.info("Request Hashtags: That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");  
    }


    /** A bit Faster than userListWithFullUser because we get less values in user (only hashtag, username is revelant)
        27.000ms with hbase writing for one day
     **/
    public static void usersList (boolean allFiles) {
            
        long startTime = System.currentTimeMillis();
        
        JavaPairRDD<String, User> users = null;

        if (allFiles){
            openFiles();
            users = files						
                    .get(0)
                    .mapToPair(line -> JsonUserReader.usernameAndHashtagFromNLJSON(line))
                    .filter( val ->  val._1() != "")
                    .filter( val -> val._2()._hashtags().size() > 0)
                    .reduceByKey((user1, user2 ) -> {
                        user1.addHashtagsUsed(user2._hashtags());
                        return user1;
                    });
  
            for (int i = 1; i < files.size(); i++) {
                users = users.union(
                        files
                        .get(i)
                        .mapToPair(line -> JsonUserReader.usernameAndHashtagFromNLJSON(line))
                        .filter( val ->  val._1() != "")
                        .filter( val -> val._2()._hashtags().size() > 0)
                        .reduceByKey((user1, user2 ) -> {
                            user1.addHashtagsUsed(user2._hashtags());
                            return user1;
                        }).unpersist());
            }   
            users = users.reduceByKey((user1, user2 ) -> {
                        user1.addHashtagsUsed(user2._hashtags());
                        return user1;
                    });
        }
        else{
            users = file
                    .mapToPair(line -> JsonUserReader.usernameAndHashtagFromNLJSON(line))
                    .filter( val ->  val._1() != "")
                    .filter( val -> val._2()._hashtags().size() > 0)
                    .reduceByKey((user1, user2 ) -> {
                        user1.addHashtagsUsed(user2._hashtags());
                        return user1;
                    });
        }
        
        // Simplement enregister le set de clés (correspondants aux usernames) dans hbase 
        // i.e  users.keys()
        //System.out.println(users.collectAsMap()); // A enlever après fais exploser la mémoire
        //logger.info("Give a sample example: 10 users: ");
        //users.take(10).forEach(f -> System.out.println(f));

        // Write to hbase
        
        users.foreach(tuple -> hbaseUserHashtags.writeTable(tuple));

        if (allFiles) {
            // On finit le travail
            files.clear();
        }
        users.unpersist(); // pas sur que ça ait un effet mais on sait jamais
            
        long endTime = System.currentTimeMillis();
        logger.info("RequestHashtags: That took without Reflexivity : (map + reduce ) " + (endTime - startTime) + " milliseconds");
        
        
    }

    public static void userListWithFullUser (boolean allFiles) {
            
        long startTime = System.currentTimeMillis();
        
        JavaPairRDD<String, User> users = null;

        if (allFiles){
            openFiles();
            users = files						
                    .get(0)
                    .mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
                    .filter( val ->  val._1() != "")
                    .filter( val -> val._2()._hashtags().size() > 0)
                    .reduceByKey((user1, user2 ) -> {
                        user1.addHashtagsUsed(user2._hashtags());
                        return user1;
                    });
  
            for (int i = 1; i < files.size(); i++) {
                users = users.union(
                        files
                        .get(i)
                        .mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
                        .filter( val ->  val._1() != "")
                        .filter( val -> val._2()._hashtags().size() > 0)
                        .reduceByKey((user1, user2 ) -> {
                            user1.addHashtagsUsed(user2._hashtags());
                            return user1;
                        }).unpersist());
            }   
            users = users.reduceByKey((user1, user2 ) -> {
                        user1.addHashtagsUsed(user2._hashtags());
                        return user1;
                    });
        }
        else{
            users = file
                    .mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
                    .filter( val ->  val._1() != "")
                    .filter( val -> val._2()._hashtags().size() > 0)
                    .reduceByKey((user1, user2 ) -> {
                        user1.addHashtagsUsed(user2._hashtags());
                        return user1;
                    });
        }
        
        // Simplement enregister le set de clés (correspondants aux usernames) dans hbase 
        // i.e  users.keys()
        //System.out.println(users.collectAsMap()); // A enlever après fais exploser la mémoire
        // logger.info("Give a sample example: 10 users: ");
        //users.take(10).forEach(f -> System.out.println(f));

        // Write to hbase
        users.foreach(tuple -> hbaseUserHashtags.writeTable(tuple));

        if (allFiles) {
            // On finit le travail
            files.clear();
        }
        users.unpersist(); // pas sur que ça ait un effet mais on sait jamais
            
        long endTime = System.currentTimeMillis();
        logger.info("RequestHashtags: That took without Reflexivity : (map + reduce + hbase ) " + (endTime - startTime) + " milliseconds");
        
        
    }


    
}
