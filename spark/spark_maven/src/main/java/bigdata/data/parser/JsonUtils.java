package bigdata.data.parser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import bigdata.data.User;
import scala.Tuple2;

public class JsonUtils {

	public final static String[] data = new String[] { "/raw_data/tweet_01_03_2020_first10000.nljson",
			"/raw_data/tweet_01_03_2020.nljson", "/raw_data/tweet_02_03_2020.nljson",
			"/raw_data/tweet_03_03_2020.nljson", "/raw_data/tweet_04_03_2020.nljson",
			"/raw_data/tweet_05_03_2020.nljson", "/raw_data/tweet_06_03_2020.nljson",
			"/raw_data/tweet_07_03_2020.nljson", "/raw_data/tweet_08_03_2020.nljson",
			"/raw_data/tweet_09_03_2020.nljson", "/raw_data/tweet_10_03_2020.nljson",
			"/raw_data/tweet_11_03_2020.nljson", "/raw_data/tweet_12_03_2020.nljson",
			"/raw_data/tweet_13_03_2020.nljson", "/raw_data/tweet_14_03_2020.nljson",
			"/raw_data/tweet_15_03_2020.nljson", "/raw_data/tweet_16_03_2020.nljson",
			"/raw_data/tweet_17_03_2020.nljson", "/raw_data/tweet_18_03_2020.nljson",
			"/raw_data/tweet_19_03_2020.nljson", "/raw_data/tweet_20_03_2020.nljson",
			"/raw_data/tweet_21_03_2020.nljson" };

	public static Iterator<String> getHashtagFromJson(String line) {
		List<String> hashs = new ArrayList<>();
		try {
			JsonObject jsonObj = new JsonParser().parse(line).getAsJsonObject();

			JsonElement entities = jsonObj.get("entities");
			if (entities != null && entities.isJsonObject()) {
				JsonElement hashtags = (entities.getAsJsonObject()).get("hashtags");
				if (hashtags != null) {
					for (JsonElement hash : hashtags.getAsJsonArray()) {
						hashs.add(hash.getAsJsonObject().get("text").getAsString());
					}
					return hashs.iterator();
				}
			}
		} catch (Exception e) {

		}
		return hashs.iterator();
	}
		
	public static Tuple2<String, User> getHashtagsFromUserInJSON(String line) {

		User user_instance = new User("", new HashSet<String>());
		String username = "";
		try {
			// Yarn compliance
			JsonElement json = new JsonParser().parse(line);
			JsonObject jsonObj = json.getAsJsonObject();
			
			// JsonObject jsonObj = JsonParser.parseString(line).getAsJsonObject();

			
			HashSet<String> hashtags = new HashSet<String>(JsonUtils.getUniqueHashtagsList(jsonObj));
			username = JsonUtils.getUsername(jsonObj);


			user_instance = new User(username, hashtags);
			
				// if (entities != null && entities.isJsonObject()) {
				// 	JsonElement user = jsonObj.get("user");
				// 	JsonElement username = user.getAsJsonObject().get("name");
				// 	JsonElement id = user.getAsJsonObject().get("id_str");
				// 	JsonElement geo = user.getAsJsonObject().get("place");

				// 	// user_instance = new User(username.getAsString(), hashs);
					
					
				// 	if(geo != null)
				// 		user_instance.setGeo(geo.getAsString() );
				// 	else
				// 		user_instance.setGeo("");
				// }

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

	private static String getGeolocalisation (JsonObject jsonObj) {
		// TODO

		return "";
	}


	@Deprecated
	public static Tuple2<String, String> getTweetLangageAndTimestamp(String line) {
		if (line.isEmpty()) {
			return new Tuple2<String, String>("", "");
		}

		try {
			JsonObject jsonObj = new JsonParser().parse(line).getAsJsonObject();

			JsonElement user = jsonObj.get("user");
			JsonElement name = user.getAsJsonObject().get("name");

			Tuple2<String, String> tuple2 = new Tuple2<String, String>(name.getAsString(), "");

			return tuple2; // jsonObj.get("lang").getAsString(),jsonObj.get("timestamp_ms").getAsString()
		} catch (Exception e) {

		}
		return null;
	}
}
