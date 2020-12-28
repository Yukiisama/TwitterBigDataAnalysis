package bigdata.data.parser;



import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class JsonUtils {

	public final static String[] data = new String[]{
		"/raw_data/tweet_01_03_2020_first10000.nljson",
		"/raw_data/tweet_01_03_2020.nljson",
		"/raw_data/tweet_02_03_2020.nljson",
		"/raw_data/tweet_03_03_2020.nljson",
		"/raw_data/tweet_04_03_2020.nljson",
		"/raw_data/tweet_05_03_2020.nljson",
		"/raw_data/tweet_06_03_2020.nljson",
		"/raw_data/tweet_07_03_2020.nljson",
		"/raw_data/tweet_08_03_2020.nljson",
		"/raw_data/tweet_09_03_2020.nljson",
		"/raw_data/tweet_10_03_2020.nljson",
		"/raw_data/tweet_11_03_2020.nljson",
		"/raw_data/tweet_12_03_2020.nljson",
		"/raw_data/tweet_13_03_2020.nljson",
		"/raw_data/tweet_14_03_2020.nljson",
		"/raw_data/tweet_15_03_2020.nljson",
		"/raw_data/tweet_16_03_2020.nljson",
		"/raw_data/tweet_17_03_2020.nljson",
		"/raw_data/tweet_18_03_2020.nljson",
		"/raw_data/tweet_19_03_2020.nljson",
		"/raw_data/tweet_20_03_2020.nljson",
		"/raw_data/tweet_21_03_2020.nljson"
	};

    public static Iterator<String> withoutReflexivityAndWholeJson(String line) {
		List<String> hashs = new ArrayList<>();
		
		JsonObject jsonObj = new JsonParser().parse(line).getAsJsonObject();

		JsonElement entities = jsonObj.get("entities");
		if (entities != null && entities.isJsonObject()) {
			JsonElement user = jsonObj.get("user");
			JsonElement name = user.getAsJsonObject().get("name");
			JsonElement id = user.getAsJsonObject().get("id_str");
			JsonElement hashtags = (entities.getAsJsonObject()).get("hashtags");
			if (hashtags != null) {
				for (JsonElement hash : hashtags.getAsJsonArray()) {
					hashs.add(hash.getAsJsonObject().get("text").getAsString());
				}
				return hashs.iterator();
			}
		}
		return hashs.iterator();
    }
    

    public static Tuple2 <String, HashSet<String>> getHashtagsFromUserInJSON(String requested_user_id, String line) {
		Tuple2 <String, HashSet<String>> res = new Tuple2<String, HashSet<String>>(requested_user_id, new HashSet<String>());

		HashSet<String> hashs = new HashSet<>();
		
		JsonObject jsonObj = new JsonParser().parse(line).getAsJsonObject();

		JsonElement users = jsonObj.get("user");
		JsonElement entities = jsonObj.get("entities");
		if (entities != null && entities.isJsonObject()) {
            JsonElement hashtags = (entities.getAsJsonObject()).get("hashtags");
			String user_id = (users.getAsJsonObject().get("name")).getAsString();
			
			if(!user_id.equals(requested_user_id)) {
				return res;
			}
            
			if (hashtags != null) {
				for (JsonElement hash : hashtags.getAsJsonArray()) {
					hashs.add(hash.getAsJsonObject().get("text").getAsString());
                }
            }
            
			res = new Tuple2<String, HashSet<String>>(jsonObj.get("id").getAsString(), hashs);
		}


		return res;
	}



    public static Tuple2 <String, String> getTweetLangageAndTimestamp( String line ) {
		if(line.isEmpty()){
			return new Tuple2<String,String>("","");
		}

		
		JsonObject jsonObj = JsonParser.parseString(line).getAsJsonObject();


		
		JsonElement user = jsonObj.get("user");
		JsonElement name = user.getAsJsonObject().get("name");

		Tuple2<String, String> tuple2 = new Tuple2<String, String>(name.getAsString(), "");

		return tuple2; //jsonObj.get("lang").getAsString(),jsonObj.get("timestamp_ms").getAsString()
	}
}
