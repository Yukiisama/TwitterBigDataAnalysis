package bigdata.requests.users;


// import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import bigdata.data.User;
import bigdata.data.parser.JsonUtils;
import scala.Tuple2;

public class RequestUsers {
    
	/**
	 * 
	 * @param user_id
	 */
	public static void UserUniqueHashtagsList (String user_id) {
		// // Input checking
		// Pattern p = Pattern.compile("\d{19}");
		// Matcher m = p.matcher(user_id);
		// if(!m.find()) {
		// 	System.err.println("[ERROR] Invalid input in mostUseHashtag@void, user_id must be a string matching regex r'\d{19}'.");
		// 	return;
		// }

		// Time calculation
		long startTime = System.currentTimeMillis();



		// Content
		JavaRDD<Tuple2<String, User>> users = file.mapToPair(line -> JsonUtils.getHashtagsFromUserInJSON(user_id, line));//.reduceByKey((a, b) -> a + b);;
		
		
		if(users == null) {
			System.err.println("[ERROR] Null dataset in UserUniqueHashtagsList@void, user probably don't have used any hashtags.");
		}

		users.reduceByKey(new Function2<Tuple2<String, User>, Tuple2<String, User>, User>() {
			public User call (Tuple2<String, User> a , Tuple2<String, User> b) {
				if(a._1().equals(b._1()))	
					a._2().setNb_tweets(a._2().getNb_tweets() + b._2().getNb_tweets());
				return a._2();	
			}
		});

		List<User> data = users.collect();


		// Output
		for(User u : data){
			System.out.println(u);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + topK) " + (endTime - startTime) + " milliseconds");
	}



	public static void UserNumberOfTweets (String user_id) {
		
	}


	/**
	 * 
	 */
	public static void TweetsPerLangagesAndTimestamp (String lang) {
		
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
	public static void UserMostRetweetedTweet (String user_id) {

	}

	/**
	 * OPTIONAL
	 * 
	 * Return the tweet from an user that has got the highest
	 * number of likes
	 */
	public static void UserMostFavoredTweet (String user_id) {

	}


	/**
	 * OPTIONAL
	 * 
	 * Return the count of localisations where the user
	 * has been the most
	 */
	public static void UserGetMostTweetedPlace (String user_id) {

	}
}
