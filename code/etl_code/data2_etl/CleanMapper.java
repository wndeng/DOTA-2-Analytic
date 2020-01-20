import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import com.google.gson.*;

import java.util.*; 

public class CleanMapper
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
    	if(m.human_players == 10) context.write(new Text(""), new Text(gson.toJson(m)));

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
  	JsonArray players;
  	JsonArray objectives;
}