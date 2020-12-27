package bigdata.file.json;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
}
