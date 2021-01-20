package bigdata;


import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.data.parser.JsonUtils;
import bigdata.infrastructure.database.runners.HBaseUser;
import bigdata.requests.EntryPoint;
import bigdata.requests.arguments.ArgumentsManager;

public class TPSpark {

    public static SparkConf conf = null;

    public static JavaRDD<String> file = null;
    public static ArrayList<JavaRDD<String>> files = new ArrayList<>();

    public static JavaSparkContext context = null;

	public static boolean __HASHTAGS__ = false;
	public static boolean __USERS__ = false;
	public static boolean __INFLUENCERS__ = false;
	public static boolean __FRESH_HBASE__ = false;
    public static boolean __HELP__ = false;

    public static final boolean __PROGRESS_BAR__ = false; // Disable if large dataset and want performances. Does not work with yarn.
    

    public static Logger logger = Logger.getLogger(TPSpark.class);

    public static  void openFiles(){
        for (int i = 1; i < JsonUtils.data.length; i++) 
            files.add(context.textFile(JsonUtils.data[i]));

        // ATTENTION je clear files à la fin de la fonction EntryPoint.HASHTAGS_BEST_ALL_FILES_TOPK.apply(10);
        // Il faudra les re-ouvrir dans les context (voir ligne 71 RequestsHashtags)
    }

    static {


        logger.info("Creating Spark Context...");
        conf = new SparkConf()
                .setAppName("TP Spark")
                .set("spark.executor.instances", "20")
                .set("spark.executor.cores", "4")
                .set("spark.shuffle.memoryFraction", "0.8")
                .set("spark.ui.showConsoleProgress ", "true")
                .set("spark.executor.extraJavaOptions", "-Dlog4j.debug=true");


        context = new JavaSparkContext(conf);
        context.defaultParallelism();
        context.setLogLevel("ERROR");

        

        // context.getLogger().removeAppender("log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle");
        // context.getLogger().removeAppender("log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper");
        // context.getLogger().removeAppender("log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter");
        logger.setLevel(Level.ALL);
        


        file = context.textFile("/raw_data/tweet_01_03_2020.nljson");
        // file = context.textFile("/raw_data/tweet_01_03_2020.nljson");
        // System.out.println("There is " + context.sc().statusTracker().getExecutorInfos().length + " Workers.");
        // // file = context.textFile(JsonUtils.data[1]);
        // file = context.textFile("/raw_data/tweet_05_03_2020.nljson");

        logger.info("Done.");


        logger.info("Creating HBase Table Manager...");
        logger.debug(" - HBaseUser...");
        //HBaseUser.INSTANCE();
        logger.info("Done.");
    }
    public static void main (String[] args) {

        String tmp = "";
        for(int i = 0; i < args.length; i++) {
            tmp.concat(args[i]);
        }
        logger.debug("Parsing Program Input Arguments: [" + tmp + "]");
        ArgumentsManager.updateProgramArgument(args);
        logger.debug("Done.");


        try {
            // System.out.println("Number of partitions : " + file.getNumPartitions());
            // System.out.println("Lines Count" + file.count());
            
            // Print val
            // file.foreach(f -> System.out.println(f));
            // hashtags.foreach( f -> System.out.println(f));
            // r.foreach(f -> System.out.println(f));




            AnalysisHashtags();
            //AnalysisUser(false);
            //AnalysisInfluencer();
        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            logger.info("End of the program, closing spark context...");
            // Always close the Spark Context.
            context.close();
            logger.info("Done.");

            LogManager.shutdown();

        }
    }


    /**
     * Run Hashtag Analysis
     */
    private static void AnalysisHashtags() {
        logger.info("Hashtags...");
        /**
         * DONE
         */
        logger.info("a) K Hashtags les plus utilisés avec nombre d'apparition sur un jour:");
        //EntryPoint.HASHTAGS_DAILY_TOPK.apply(10000);
        logger.info("b) K Hashtags les plus utilisés avec nombre d'apparition sur toutes les données:");
        // Question c en même temps
        EntryPoint.HASHTAGS_BEST_ALL_FILES_TOPK.apply(10000);
        
        final Boolean allFiles = true;
        logger.info("d) Utilisateurs ayant utilisé un Hashtag:");
        //EntryPoint.HASHTAGS_USED_BY.apply(allFiles);

        /**
         * Optional
         */
    }


    /**
     * Run Users Analysis
     */
    private static void AnalysisUser(boolean allFiles) {

        logger.info("Utilisateurs...");
        /**
         * DONE
         */
        // System.out.println("a) Liste des Hashtags sans doublons:");
        EntryPoint.USERS_UNIQUE_HASHTAGS_LIST.apply(allFiles);

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
        logger.info("Influenceurs...");
        logger.info("a) Récupérer tous les triplets de hashtags ainsi que les utilisateurs qui les ont utilisés");
        // Question b et c faites en même temps;
        //RequestInfluenceurs.TripleHashtag(false, true, 10000);
    }
}
