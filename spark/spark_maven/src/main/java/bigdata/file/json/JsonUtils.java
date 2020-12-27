package bigdata.file.json;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class JsonUtils {
    public static Iterator<String> withoutReflexivityAndWholeJson(String line) {
		List<String> hashs = new ArrayList<>();
		JsonElement json = new JsonParser().parse(line);
		JsonObject jsonObj = json.getAsJsonObject();
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
		return hashs.iterator();
    }
    

    public static Tuple2 <String, HashSet<String>> withoutReflexivityAndWholeJsonUsers(String requested_user_id, String line) {
		Tuple2 <String, HashSet<String>> res = new Tuple2<String, HashSet<String>>(requested_user_id, new HashSet<String>());

		HashSet<String> hashs = new HashSet<>();
		JsonElement json = new JsonParser().parse(line);
		JsonObject jsonObj = json.getAsJsonObject();
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
			

			System.out.println("Found a value:" + res);
		}
		return res;
	}
}
