package bigdata.requests.influencers;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import bigdata.data.User;
import bigdata.data.comparator.HashtagComparator;
import bigdata.data.comparator.TripleHashtagComparator;
import bigdata.data.comparator.RtComparator;
import bigdata.data.comparator.TweetComparator;
import bigdata.data.parser.JsonUserReader;
import bigdata.data.parser.JsonUtils;
import scala.Tuple2;
import scala.Tuple3;
import bigdata.infrastructure.database.runners.HBaseTopKTripleHashtag;
import bigdata.infrastructure.database.runners.HBaseInfluenceurs;
import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;
import static bigdata.TPSpark.files;
import static bigdata.TPSpark.openFiles;
import static bigdata.TPSpark.logger;

public class RequestInfluenceurs {
    // TopK to true compute the topK (question b & c) and question a
    // If false only question a (triple hashtag as key, users as value)
    
    public static HBaseTopKTripleHashtag tripleHashHbase = new HBaseTopKTripleHashtag("TripleHashtags");
    public static HBaseInfluenceurs influenceursHbase = HBaseInfluenceurs.INSTANCE();
    
    public static void TripleHashtag (boolean allFiles, boolean topK, int k) {
        
        long startTime = System.currentTimeMillis();
        
        JavaPairRDD<Tuple3<String,String,String>, Set<User>> users = null;
        JavaPairRDD<Tuple3<String,String,String>, Integer> mapredTopKTripleHashtag = null;
        if (allFiles){
            openFiles();
            JavaPairRDD<String, User> r = filterTriple(true, 0);        
            users = mapReduceTripleByUser(r);

           if (topK) {
               mapredTopKTripleHashtag = mapReduceTripleTopK(r);  		   
           }
                 
  
          for (int i = 1; i < files.size(); i++) {
                JavaPairRDD<String, User> temp = filterTriple(true, i);
                
                users = users.union(mapReduceTripleByUser(temp));
                        
                if (topK) {
                    mapredTopKTripleHashtag = mapredTopKTripleHashtag.union(mapReduceTripleTopK(temp));   
                }
            }   
            
            users = users.reduceByKey((user1, user2 ) -> {
                user1.addAll(user2);
                return user1;
            });
            
            if (topK) {
                mapredTopKTripleHashtag = mapredTopKTripleHashtag.reduceByKey((a, b) -> a + b);
            }
            
        }
        else {
            JavaPairRDD<String, User> r = filterTriple(false, 0);         
            users = mapReduceTripleByUser(r);
            if (topK) {
                mapredTopKTripleHashtag = mapReduceTripleTopK(r);  		   
            }
        }
        
        //users.take(10).forEach(f -> System.out.println(f));
        logger.info("Printing a sample, 100 triple hashtags +  users");
        users.take(100).forEach(f ->{
            System.out.println();
            System.out.print(f._1 + " : [");
            f._2().forEach(u -> System.out.print(u._id() + ", "));
            System.out.print("]");
        });
        // users -> question a) à enregistrer dans hbase
        if (topK) {
            System.out.println();
            logger.info(" b) Donner k triplets de hashtags les plus utilisés");
            List<Tuple2<Tuple3<String,String,String>, Integer>> top = mapredTopKTripleHashtag.top(k, new TripleHashtagComparator());  
            logger.info("Printing a sample, the first 100 of the topk triple hashtag");
            for (int i = 0; i < 100 ; i++){
                System.out.println(top.get(i));
            }

            // Hbase
            logger.info("Writing to hbase triple hashtags with their count ...");
            top.forEach(tuple -> tripleHashHbase.writeTable(tuple));
            logger.info("Writing Done");

            // Question c) Sur le topk
            System.out.println("Question C: Trouver les influencersc.a.d les personnes avec le plus grand nombre de tweets dans les triplets que l’on a trouvé.");
            JavaRDD<User> us = (users.join(context.parallelizePairs(top)))
            .unpersist()
            .flatMap(val -> val._2._1().iterator());

            logger.info("Calculating topk influenceurs ...");
            List<Tuple2<String, Integer>> influenceurs = calculateInfluenceurs(us, k);
            
            logger.info("Printing a sample, the first 100 of the topk influenceurs");
            for (int i = 0; i < 100 ; i++){
                System.out.println(influenceurs.get(i));
            }

            // Hbase
            logger.info("Writing to hbase influenceurs with their number of messages ...");
            influenceurs.forEach(tuple -> influenceursHbase.writeTable(tuple));
            logger.info("Writing Done");

            logger.info("Question d : Trouver les faux influencer, personnes avec beaucoup de followers dont les tweets ne sont jamais retweeté.");
            List<Tuple2<String, User>> fakeInfluenceurs = calculateFakeInfluenceurs(us, k);
            fakeInfluenceurs.forEach(f -> System.out.println(f));
            
        }
        
        if (allFiles) {
            // On finit le travail
            users.unpersist(); // pas sur que ça ait un effet mais on sait jamais
            files.clear();
        }
            
        long endTime = System.currentTimeMillis();
        System.out.println("That took without Reflexivity : (map + reduce ) " + (endTime - startTime) + " milliseconds");
        
        
    }
    
    private static List<Tuple2<String, User>> calculateFakeInfluenceurs(JavaRDD<User> UserRdd, int k) {    
        List<Tuple2<String, User>> influenceurs = UserRdd.mapToPair(val -> {
            	return new Tuple2<String, User>(val._id(), val);
            })
            .reduceByKey((a, b) -> {
                a.setNbTweets(a._nbTweets() + b._nbTweets());
                a.setReceivedRTs(a._received_rts() + b._received_rts());
                return a;
            })
            .top(k, new TweetComparator()); // compare nb tweets in order to find influenceurs
            
            List<Tuple2<String, User>> fakeInfluenceurs = context.parallelizePairs(influenceurs)
            .top(k, new RtComparator()); // compare  nb retweet in order in influenceurs topk to find fake influenceurs
           
        return fakeInfluenceurs;
    }

    private static List<Tuple2<String, Integer>> calculateInfluenceurs(JavaRDD<User> UserRdd, int k) {
        return UserRdd.mapToPair(val -> {
            	return new Tuple2<String, Integer>(val._id(), val._nbTweets());
            })
            .reduceByKey((a, b) -> a + b)
            .top(k, new HashtagComparator()); // compare counts
           
    }

    private static JavaPairRDD<String, User> filterTriple(boolean allFiles, int i){
        return ((allFiles) ? files.get(i) : file)				
                .mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
                .filter( val ->  val._1() != "")
                .filter( val -> val._2()._hashtags().size() == 3);
    }
    
    private static JavaPairRDD<Tuple3<String,String,String>, Set<User>> mapReduceTripleByUser(JavaPairRDD<String, User> r) {
        return r.mapToPair( val -> {
            String[] s = new String[3];
            val._2._hashtags().keySet().toArray(s);
            Set<User> set = new HashSet<>();
            set.add(val._2);
            return new Tuple2<Tuple3<String,String,String>, Set<User>>(new Tuple3<>(s[0], s[1], s[2]), set);
        })
        .reduceByKey((user1, user2 ) -> {
            user1.addAll(user2);
            return user1;
        });
    }
    
    private static JavaPairRDD<Tuple3<String, String, String>, Integer> mapReduceTripleTopK(JavaPairRDD<String, User> r) {
        return r.mapToPair( val -> {
            String[] s = new String[3];
            val._2._hashtags().keySet().toArray(s);
            return new Tuple2<Tuple3<String,String,String>, Integer>(new Tuple3<>(s[0], s[1], s[2]), 1);
        })
        .reduceByKey((a, b) -> a + b);
    }
}
