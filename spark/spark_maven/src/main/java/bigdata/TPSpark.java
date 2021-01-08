package bigdata;


import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import bigdata.infrastructure.database.runners.HBaseUser;
import bigdata.infrastructure.database.runners.HBaseTopKHashtag;
import bigdata.requests.EntryPoint;
import bigdata.requests.influencers.RequestInfluenceurs;
import bigdata.data.parser.JsonUtils;

public class TPSpark {

    public static SparkConf conf = null;

    public static JavaRDD<String> file = null;
    public static ArrayList<JavaRDD<String>> files = new ArrayList<>();

    public static JavaSparkContext context = null;

    public static  void openFiles(){
        for (int i = 1; i < JsonUtils.data.length; i++) 
            files.add(context.textFile(JsonUtils.data[i]));
        // ATTENTION je clear files à la fin de la fonction EntryPoint.HASHTAGS_BEST_ALL_FILES_TOPK.apply(10);
        // Il faudra les re-ouvrir dans les context (voir ligne 71 RequestsHashtags)
    }

    public static HBaseUser database_user = HBaseUser.INSTANCE();

    static {
        conf = new SparkConf()
                .setAppName("TP Spark")
                .set("spark.executor.instances", "20")
                .set("spark.executor.cores", "4")
                .set("spark.shuffle.memoryFraction", "0.8");

        context = new JavaSparkContext(conf);
        context.defaultParallelism();
        context.setLogLevel("ERROR");


        //file = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
        file = context.textFile("/raw_data/tweet_01_03_2020.nljson");
        // System.out.println("There is " + context.sc().statusTracker().getExecutorInfos().length + " Workers.");
        // // file = context.textFile(JsonUtils.data[1]);
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
            //AnalysisUser();
            //AnalysisInfluencer();
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
        EntryPoint.HASHTAGS_DAILY_TOPK.apply(10000);
        System.out.println("b) K Hashtags les plus utilisés avec nombre d'apparition sur toutes les données:");
        // Question c en même temps
        //EntryPoint.HASHTAGS_BEST_ALL_FILES_TOPK.apply(10);
        
        final Boolean allFiles = true;
        System.out.println("d) Utilisateurs ayant utilisé un Hashtag:");
        //EntryPoint.HASHTAGS_USED_BY.apply(allFiles);

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
    
    private static void AnalysisInfluencer() {
        System.out.println("a) Récupérer tous les triplets de hashtags ainsi que les utilisateurs qui les ont utilisés");
        // Question b et c faites en même temps;
        //RequestInfluenceurs.TripleHashtag(false, true, 10000);
    }
}
