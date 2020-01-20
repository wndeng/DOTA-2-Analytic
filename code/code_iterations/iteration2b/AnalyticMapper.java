import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import com.google.gson.*;
import java.util.*; 

public class AnalyticMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void setup(Context context) {
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
		String s = value.toString();
		if(s.length() == 1) return;

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
    	Matches m = gson.fromJson(s, Matches.class);
    	if(m.lobby_type == 0 || m.lobby_type == 2 || m.lobby_type == 5 || m.lobby_type == 6 || m.lobby_type == 7 || m.human_players == 10 && m.radiant_win != null) {
    		String a = "";
    		a += m.radiant_win ? "true," : "false,";
    		a += Integer.toString(m.game_mode)+',';
    		a += Integer.toString(m.duration)+',';
    		if(m.radiant_gold_adv != null && m.radiant_gold_adv.length >= 10) {
    			a += Integer.toString(m.radiant_gold_adv[9])+',';
    			a += Integer.toString(m.radiant_xp_adv[9])+',';
    		}
    		else a += "null,null,";
    		JsonArray pl = m.players.getAsJsonArray();
    		ArrayList objList = gson.fromJson(pl, ArrayList.class);
    		if(objList.size() != 10) return;
    		for (int i = 0; i < 10; ++i) {
    			Players p = gson.fromJson(gson.toJson(objList.get(i)), Players.class);
			if(p.leaver_status >= 1) return;
    			a += Integer.toString(p.hero_id)+',';
    			a += Integer.toString(p.kills)+',';
	    		a += Integer.toString(p.assists)+',';
	    		a += Integer.toString(p.hero_damage)+',';
	    		a += Integer.toString(p.gold_per_min)+',';
	    		a += Integer.toString(p.xp_per_min);
	    		if(i != 9) a += ',';
    		}
    		context.write(new Text(Integer.toString(m.match_id)), new Text(a));
    	}
	}
}

class Matches {
	Integer match_id;
	Boolean radiant_win;
	Integer human_players;
	Integer game_mode;
	Integer duration;
	Integer[] radiant_gold_adv;
  	Integer[] radiant_xp_adv;
	Integer lobby_type;
  	JsonElement players;
}

class Players {
	Integer hero_id;
	Integer kills;
	Integer assists;
	Integer hero_damage;
	Integer gold_per_min;
	Integer xp_per_min;
	Integer  leaver_status;
}
