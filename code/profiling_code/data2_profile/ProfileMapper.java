import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import com.google.gson.*;

import java.util.*; 

public class ProfileMapper
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
    	context.write(new Text("match_id"), new Text(String.valueOf(m.match_id)));
    	context.write(new Text("game_mode"), new Text(String.valueOf(m.game_mode)));
    	context.write(new Text("duration"), new Text(String.valueOf(m.duration)));
    	if(m.radiant_gold_adv != null) context.write(new Text("gold_adv"), new Text(String.valueOf(m.radiant_gold_adv.length)));
    	if(m.radiant_xp_adv != null) context.write(new Text("xp_adv"), new Text(String.valueOf(m.radiant_xp_adv.length)));
    	// context.write(new Text("players"), new Text(String.valueOf(m.players.size())));
    	// context.write(new Text("objectives"), new Text(String.valueOf(m.objectives.size())));

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
  	JsonElement players;
  	JsonElement objectives;
}