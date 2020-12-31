package bigdata.requests.users;

import static bigdata.TPSpark.files;
import static bigdata.TPSpark.openFiles;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import bigdata.data.User;
import bigdata.data.parser.JsonUserReader;
import bigdata.infrastructure.database.runners.HBaseUser;
import scala.Tuple2;

public class RequestUsers {
    
	/**
	 * 
	 * @param user_id
	 */
	public static void UserUniqueHashtagsList () {
		// Time calculation
		long startTime = System.currentTimeMillis();


		// Content
		/**
		 * Multiple Files
		 */
		openFiles();

		JavaPairRDD<String, User> tuple_users = files.get(0).mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
		.filter(new Function<Tuple2<String, User>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, User> val) throws Exception {
				return val._1() != "";
			}
		});

		/**
		 * Single File
		 */
		// JavaPairRDD<String, User> tuple_users = file.mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
		// 	.filter(new Function<Tuple2<String, User>, Boolean>() {
		// 		@Override
		// 		public Boolean call(Tuple2<String, User> val) throws Exception {
		// 			return val._1() != "";
		// 		}
		// 	});
		

		if(tuple_users == null) {
			System.err.println("[ERROR] Null dataset in @void, user probably doesn't have used any hashtags.");
		}



		/**
		 * Reduce and merge data
		 */
		JavaPairRDD<String, User> users = tuple_users.reduceByKey(new Function2<User, User, User>() {
			public User call (User a, User b) {
				a.setNbTweets(a._nbTweets() + b._nbTweets());
				a.setReceivedFavs(a._received_favs() + b._received_favs());
				a.setReceivedRTs(a._received_rts() + b._received_rts());

				a.addGeo(b._geos());
				a.addLangUsed(b._langs());
				a.addPublicationSource(b._sources());
				a.addDailyFrequencyData(b._frequencies());

				return a;	
			}
		});
		tuple_users.unpersist();









		/*
		 *
		 * Output inside HBase using RDD (opti)
		 * 
		 */
		// final JavaPairRDD<ImmutableBytesWritable, Put> hbaseRDD = users.mapToPair(row -> row._2.getPuts());
		// System.out.println("config:" + HBaseUser.INSTANCE().config);
		// // List<Tuple2<ImmutableBytesWritable, Put>> data = hbaseRDD.take(10); // j'en prends que 10 pour pas exploser la mémoire mais
		// hbaseRDD.saveAsNewAPIHadoopDataset(HBaseUser.INSTANCE().config);
		// // hbaseRDD.unpersist();


		/**
		 * Output inside HBase using RDD (intermediate opti)
		 */
		users.foreach(tuple -> HBaseUser.INSTANCE().writeTable(tuple._2));


		/*
		 *
		 * Output inside HBase Using Data Structure (Not opti)
		 * 
		 */
		// List<Tuple2<String, User>> data = users.collect(); //explose la mémoire sur json entier
		// for(Tuple2<String, User> tuple : data) {
		// }
		
		long endTime = System.currentTimeMillis();
		System.out.println("That took without Reflexivity : (map + reduce + HBase) " + (endTime - startTime) + " milliseconds");
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
