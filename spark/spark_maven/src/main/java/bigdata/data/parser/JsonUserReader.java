package bigdata.data.parser;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import bigdata.data.User;
import scala.Tuple2;


public class JsonUserReader {
    

	public static Tuple2<String, User> readDataFromNLJSON(String line) {
		
		// String user_UUID = "";
		String username = "";
		String user_UUID ="";
		User user_instance = new User(user_UUID, username, new HashSet<String>());


		try {
			
			/*
			* Both of following calls depends of the Gson version used during compilation.
			*/
			// Yarn compliance
			JsonElement json = new JsonParser().parse(line);
			JsonObject jsonObj = json.getAsJsonObject();
			
			// Non-Yarn
			// JsonObject jsonObj = JsonParser.parseString(line).getAsJsonObject();

			
			HashSet<String> hashtags = new HashSet<String>(JsonUserReader.getUniqueHashtagsList(json.getAsJsonObject()));
			username = JsonUserReader.getUsername(json.getAsJsonObject());
			user_UUID = JsonUserReader.getUserUUID(json.getAsJsonObject());

			user_instance = new User(user_UUID, username, hashtags);
			user_instance.setReceivedFavs(JsonUserReader.getFavoriteCount(json.getAsJsonObject()));
			user_instance.setReceivedRTs(JsonUserReader.getRetweetCount(json.getAsJsonObject()));
			user_instance.addPublicationSource(JsonUserReader.getTweetSource(json.getAsJsonObject()));
			user_instance.addLangUsed(JsonUserReader.getLangage(json.getAsJsonObject()));
			user_instance.addDailyFrequencyData(JsonUserReader.getDatePosted(json.getAsJsonObject()));
			user_instance.addGeo(JsonUserReader.getGeolocalisation(json.getAsJsonObject()));

			// user_UUID = JsonUserReader.getUserUUID(jsonObj);
			

		} catch(Exception e) {

		} finally {
			return new Tuple2 <String, User> (username, user_instance);
		}
	}

	private static Set<String> getUniqueHashtagsList(JsonObject jsonObj) {
		HashSet<String> hashs = new HashSet<>();

		// Fail-safe
		if(jsonObj == null) {
			return hashs;
		}


		JsonElement entities = jsonObj.get("entities");

		if (entities != null && entities.isJsonObject()) {

			JsonElement hashtags = (entities.getAsJsonObject()).get("hashtags");

			if (hashtags != null) {

				for (JsonElement hash : hashtags.getAsJsonArray()) {
					hashs.add(hash.getAsJsonObject().get("text").getAsString());
				}

			}
		}

		return hashs;
	}

	private static String getUsername (JsonObject jsonObj) {
		String res = "";

		
		JsonElement user = jsonObj.get("user");

		// Fail-safe
		if(user == null) {
			return res;
		}

		JsonElement username = user.getAsJsonObject().get("name");


		res = username.getAsString();
		if (res == null) {
			res = "";
		}

		return res;
	}


	
	private static String getUserUUID (JsonObject jsonObj) {
		String res = "";

		
		JsonElement user = jsonObj.get("user");

		// Fail-safe
		if(user == null) {
			return res;
		}

		JsonElement username = user.getAsJsonObject().get("id_str");


		res = username.getAsString();
		if (res == null) {
			res = "";
		}

		return res;
	}

	private static String getGeolocalisation (JsonObject jsonObj) {

		return jsonObj.get("place").getAsString();
	}


	private static int getFavoriteCount (JsonObject jsonObj) {
		int res = jsonObj.get("favorite_count").getAsInt();

		return res;
	}

	private static int getRetweetCount (JsonObject jsonObj) {
		int res = jsonObj.get("retweet_count").getAsInt();

		return res;
	}


	private static String getLangage (JsonObject jsonObj) {
		String res = jsonObj.get("lang").getAsString();

		return res;
	}

	private static String getTweetSource (JsonObject jsonObj) {
		String res = jsonObj.get("source").getAsString();

		return res;
	}
	

	private static Date getDatePosted (JsonObject jsonObj) {
		Timestamp stamp = new Timestamp(jsonObj.get("timestamp_ms").getAsLong());

		Date d = new Date(stamp.getTime());

		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);

		Date res = cal.getTime();


		return res;

	}
}
