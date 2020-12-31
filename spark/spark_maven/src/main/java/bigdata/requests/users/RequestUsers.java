package bigdata.requests.users;


// import static bigdata.TPSpark.context;
import static bigdata.TPSpark.file;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
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


  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * @param conf Configuration for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param fClass Class of the InputFormat
   * @param kClass Class of the keys
   * @param vClass Class of the values
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
		final JavaPairRDD<ImmutableBytesWritable, Put> hbaseRDD = users.mapToPair(row -> row._2.getPuts());



		// Output inside stdout
		// for(Tuple2<String, User> tuple : users.sample(false, 1/1000)) {
		// 	System.out.println(tuple._2);
		// }
		// users.unpersist();


		// Output inside HBase
		System.out.println("config:" + HBaseUser.INSTANCE().config);
		// List<Tuple2<ImmutableBytesWritable, Put>> data = hbaseRDD.take(10); // j'en prends que 10 pour pas exploser la mémoire mais
		hbaseRDD.saveAsNewAPIHadoopDataset(HBaseUser.INSTANCE().config);
		// hbaseRDD.unpersist();


		// List<Tuple2<String, User>> data = users.collect(); //explose la mémoire sur json entier
		// List<Tuple2<ImmutableBytesWritable, Put>> data = hbaseRDD.take(10); // j'en prends que 10 pour pas exploser la mémoire mais
														  // faudra écrire tout users dans hbase

		
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
