import java.io.FileReader; 
import java.util.Iterator; 
import java.util.Map; 
import com.google.gson.*;
import java.io.IOException;
import java.util.ArrayList;


public class HeroParse
{
    public static void main(String[] args) throws IOException {
    	Gson gson = new GsonBuilder().create();
    	Tmp t = gson.fromJson(new FileReader("heroes.json"), Tmp.class);

    	JsonArray obj = t.heroes.getAsJsonArray();
    	ArrayList objList = gson.fromJson(obj, ArrayList.class);
    	for(int i = 0; i < 112; i++) {
    		Hero h = gson.fromJson(gson.toJson(objList.get(i)), Hero.class);
    		System.out.println(Integer.toString(h.id) + "," + h.localized_name);
    	}
    }
}

class Tmp {
	JsonArray heroes;
}

class Hero {
	Integer id;
	String localized_name;
}
