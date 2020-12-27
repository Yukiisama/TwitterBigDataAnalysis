package bigdata.requests.users;


import java.util.HashSet;
import java.util.Map;


import org.apache.spark.api.java.JavaPairRDD;

import bigdata.data.parser.JsonUtils;


// import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;

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
		JavaPairRDD<String, HashSet<String>> user_hashtags = file.mapToPair(line 
				-> JsonUtils.getHashtagsFromUserInJSON(user_id, line));
		
		if(user_hashtags == null) {
			System.err.println("[ERROR] Null dataset in UserUniqueHashtagsList@void, user probably don't have used any hashtags.");
		}

		Map<String, HashSet<String>> data = user_hashtags.collectAsMap();

		// Output
		System.out.println(data);
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
