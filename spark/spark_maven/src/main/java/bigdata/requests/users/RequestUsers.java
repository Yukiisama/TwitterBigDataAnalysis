package bigdata.requests.users;


// import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
		JavaPairRDD<String, User> tuple_users = file.mapToPair(line -> JsonUserReader.readDataFromNLJSON(line))
			.filter(new Function<Tuple2<String, User>, Boolean>() {
				@Override
				public Boolean call(Tuple2<String, User> val) throws Exception {
					return val._1() != "";
				}
			});//.reduceByKey((a, b) -> a + b);;
		

		if(tuple_users == null) {
			System.err.println("[ERROR] Null dataset in @void, user probably doesn't have used any hashtags.");
		}


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


		final JavaPairRDD<ImmutableBytesWritable, Put> hbaseRDD = users.mapToPair(row -> row._2.getPuts());


		// Output inside stdout
		// for(Tuple2<String, User> tuple : users.sample(false, 1/1000)) {
		// 	System.out.println(tuple._2);
		// }
		users.unpersist();


		// Output inside HBase
		hbaseRDD.saveAsNewAPIHadoopDataset(HBaseUser.INSTANCE().getConf());
		hbaseRDD.unpersist();


		// List<Tuple2<String, User>> data = users.collect(); //explose la mémoire sur json entier
		// List<Tuple2<String, User>> data = users.take(10); // j'en prends que 10 pour pas exploser la mémoire mais
		// 												  // faudra écrire tout users dans hbase

		
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
