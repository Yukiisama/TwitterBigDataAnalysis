package bigdata.data.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;



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
}
