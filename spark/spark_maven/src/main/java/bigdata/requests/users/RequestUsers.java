package bigdata.requests.users;

import static bigdata.TPSpark.file;
import static bigdata.TPSpark.files;
import static bigdata.TPSpark.logger;
import static bigdata.TPSpark.openFiles;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import bigdata.TPSpark;
import bigdata.data.User;
import bigdata.data.parser.JsonUserReader;
import bigdata.infrastructure.database.runners.HBaseUser;
import scala.Tuple2;



public class RequestUsers {
    
	/**
	 * 
	 * @param user_id
	 */
	public static void UserUniqueHashtagsList (boolean allFiles) {
		// Time calculation
		long startTime = System.currentTimeMillis();

		// allFiles = false;


		// Content
		/**
		 * Multiple Files
		 */
		JavaPairRDD<String, User> tuple_users = null;

		if(allFiles){
			logger.info("Opening the whole dataset.");

			logger.debug("Opening every Files...");
			openFiles();
			logger.debug("Done.");

			logger.debug("Converting Files to RDD...");
			tuple_users = files.get(0).mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
			.filter(new Function<Tuple2<String, User>, Boolean>() {
				@Override
				public Boolean call(Tuple2<String, User> val) throws Exception {
					return val._1() != "";
				}
			});
			logger.debug("Done.");

		} else {
			logger.info("Opening a sample dataset.");
			
			/**
			 * Single File of 10k lines
			 */
			logger.debug("Converting File to RDD...");
			tuple_users = file.mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
			.filter(new Function<Tuple2<String, User>, Boolean>() {
				@Override
				public Boolean call(Tuple2<String, User> val) throws Exception {
					return val._1() != "";
				}
			});
			logger.debug("Done.");
	
		}


		if(tuple_users == null) {
			logger.fatal("Null dataset in @void, user probably doesn't have used any hashtags or invalid file(s).");
		}



		/**
		 * Reduce and merge data
		 *
		 * Should be using a function as merge(User u)#User
		 * to offers more resilience.
		 *
		 */
		logger.debug("Reducing user RDD and merging values...");
		JavaPairRDD<String, User> users = tuple_users.reduceByKey(new Function2<User, User, User>() {
			public User call (User a, User b) {
				return User.merge(a, b);	
			}
		});
		logger.debug("Done.");
		tuple_users.unpersist();


		if(!RDDToServingLayer(users)){
			logger.fatal("Unable to save the Spark RDD for Users to the Serving Layer.");
		}
		
		long endTime = System.currentTimeMillis();

		logger.info("Request Users: That took without Reflexivity : (map + reduce + HBase) " + (endTime - startTime) + " milliseconds");

		if (allFiles) {
            files.clear();
		}
		
	}


	private static void printProgress(long startTime, long total, long current) {
		StringBuilder string = new StringBuilder(140);   
		int percent = (int) (current * 100 / total);
		string
			.append('\r')
			.append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " ")))
			.append(String.format(" %d%% [", percent))
			.append(String.join("", Collections.nCopies(percent, "=")))
			.append('>')
			.append(String.join("", Collections.nCopies(100 - percent, " ")))
			.append(']')
			.append(String.join("", Collections.nCopies((int) (Math.log10(total)) - (int) (Math.log10(current)), " ")))
			.append(String.format(" %d/%d", current, total));
	
		System.out.print(string);
	}


	private static long current_progression = 0;
	private static long size_rdd = 1;
	private static boolean RDDToServingLayer(JavaPairRDD<String, User> rdd) {
		logger.info("Sending RDD's data to the Serving Layer...");


		// logger.debug("Counting the number of RDD's element to send...");
		// if(TPSpark.__PROGRESS_BAR__)
			// size_rdd = rdd.count();
		// logger.debug("There is: " + size_rdd + " entries to insert.");

		try {
			/**
			 * Output inside HBase using RDD (intermediate opti)
			 */	
			// if(value > 50000){
			// 	logger.debug("The number of entries is too much important. Progress Bar will not be visible.");
			// 	rdd.foreach(tuple -> {
			// 		HBaseUser.INSTANCE().writeTable(tuple._2);
			// 	});
			// } else {
				// rdd.foreach(tuple -> {
				// 	HBaseUser.INSTANCE().writeTable(tuple._2);
				// 	current_progression = current_progression + 1;
					
				// 	printProgress(0, value, current_progression);
				// });
			// }



			if(TPSpark.__PROGRESS_BAR__){
				rdd.foreach(tuple -> {
					HBaseUser.INSTANCE().writeTable(tuple._2);
					current_progression = current_progression + 1;
					
					printProgress(0, size_rdd, current_progression);
				});
				System.out.println("");
			} else {

				rdd.foreach(tuple -> {
					try{
						HBaseUser.INSTANCE().writeTable(tuple._2);
					} catch (Exception e) {
						logger.error("unable to insert an user value in hbase.");
						logger.error(tuple._2);
					}
				});
			}

			logger.info("Done.");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Failure.");
			return false;
		} finally {
			rdd.unpersist();
		}
	}

	/*
		*
		* Output inside HBase Using Data Structure (Not opti) and might even crash on large data
		* 
		*/
	private static boolean RDDToStdout(JavaPairRDD<String, User> rdd) {
		try {
			// rdd.forEach(tuple -> System.out.println(tuple._2));

			return true;
		} catch (Exception e) {
			return false;
		}
	}





	public static void UserNumberOfTweets () {
		
	}



	/**
	 * OPTIONAL
	 * 
	 * Return the tweet from an user that has got the highest
	 * number of rt
	 */
	public static void UserMostRetweetedTweet () {

	}

	/**
	 * OPTIONAL
	 * 
	 * Return the tweet from an user that has got the highest
	 * number of likes
	 */
	public static void UserMostFavoredTweet () {

	}


	/**
	 * OPTIONAL
	 * 
	 * Return the count of localisatioByns where the user
	 * has been the most
	 */
	public static void UserGetMostTweetedPlace () {

	}
}
