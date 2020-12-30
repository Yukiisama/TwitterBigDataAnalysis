package bigdata.requests.users;


// import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;

import bigdata.data.User;
import bigdata.data.parser.JsonUtils;
import scala.Tuple2;
import java.util.HashSet;

public class RequestUsers {
    
	/**
	 * 
	 * @param user_id
	 */
	public static void UserUniqueHashtagsList () {
		// Time calculation
		long startTime = System.currentTimeMillis();



		// Content
		JavaPairRDD<String, User> tuple_users = file.mapToPair(line -> JsonUtils.getHashtagsFromUserInJSON(line))
			.filter(new Function<Tuple2<String, User>, Boolean>() {
				@Override
				public Boolean call(Tuple2<String, User> val) throws Exception {
					return val._1() != "";
				}
			});//.reduceByKey((a, b) -> a + b);;
		

		if(tuple_users == null) {
			System.err.println("[ERROR] Null dataset in UserUniqueHashtagsList@void, user probably don't have used any hashtags.");
		}


		JavaPairRDD<String,User> users = tuple_users.reduceByKey(new Function2<User, User, User>() {
			public User call (User a, User b) {
				a.setNbTweets(a._nbTweets() + b._nbTweets());

				return a;	
			}
		});

		List<Tuple2<String, User>> data = users.collect();


		// Output
		int counter = 0;
		for(Tuple2<String, User> user : data){
			if(user._2()._nbTweets() != 1)
				System.out.println(user._2());
			else
				counter = counter + 1;
		}

		System.out.println("There is " + counter + " users with a single messages on this dataset.");
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
	}



	public static void UserNumberOfTweets () {
		
	}


	/**
	 * 
	 */
	public static void TweetsPerLangagesAndTimestamp () {
		
		// Time calculation
		long startTime = System.currentTimeMillis();



		// Content
		JavaPairRDD<String, String> tweets_per_langages_and_timestamp = file.mapToPair(line 
				-> JsonUtils.getTweetLangageAndTimestamp(line));



		if(tweets_per_langages_and_timestamp == null) {
			System.err.println("[ERROR] Null dataset in UserUniqueHashtagsList@void, user probably don't have used any hashtags.");
		}

		Map<String, String> data = tweets_per_langages_and_timestamp.collectAsMap();

		// Output
		System.out.println(data);
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
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
	 * Return the count of localisations where the user
	 * has been the most
	 */
	public static void UserGetMostTweetedPlace () {

	}
}
