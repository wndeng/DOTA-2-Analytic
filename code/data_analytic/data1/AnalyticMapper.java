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
		String line = value.toString();
		if(line.length() == 1) return;
		
                // process the line.
				int i = line.indexOf("\"\"0\"\"")-2;
				if(i<=0) return ; 
                String left = line.substring(0, i);
                String right = line.substring(i+1, line.length()-1);
                
				String[] fields = left.split(",");
				if(Integer.valueOf(fields[4]) <= 600) return ;
                //System.out.println(fields[11]+" "+fields[12]);
                if(fields[11].equals("0") ||  fields[11].equals("2") || fields[11].equals("5")|| fields[11].equals("6") || fields[11].equals("7") && fields[12].equals("10")){
                    right = right.replace("\"0\"", "slot0");
                    right = right.replace("\"1\"", "slot1");
                    right = right.replace("\"2\"", "slot2");
                    right = right.replace("\"3\"", "slot3");
                    right = right.replace("\"4\"", "slot4");
                    right = right.replace("\"128\"", "slot5");
                    right = right.replace("\"129\"", "slot6");                
                    right = right.replace("\"130\"", "slot7");
                    right = right.replace("\"131\"", "slot8");
                    right = right.replace("\"132\"", "slot9");
                    right = right.replace("\"\"", "\"");
                    // System.out.println(right);
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    Players m = gson.fromJson(right, Players.class);
                    Slot s0 = gson.fromJson(m.slot0, Slot.class);
                    Slot s1 = gson.fromJson(m.slot1, Slot.class);
                    Slot s2 = gson.fromJson(m.slot2, Slot.class);
                    Slot s3 = gson.fromJson(m.slot3, Slot.class);
                    Slot s4 = gson.fromJson(m.slot4, Slot.class);
                    Slot s5 = gson.fromJson(m.slot5, Slot.class);
                    Slot s6 = gson.fromJson(m.slot6, Slot.class);
                    Slot s7 = gson.fromJson(m.slot7, Slot.class);
                    Slot s8 = gson.fromJson(m.slot8, Slot.class);
					Slot s9 = gson.fromJson(m.slot9, Slot.class);
					String win = "true";
					// Radiant 
					if(fields[2].equals("f")){
						win = "false";
					}
					context.write(new Text(fields[0]), new Text(s0.hero_id+","+win+",0"));
					context.write(new Text(fields[0]), new Text(s1.hero_id+","+win+",0"));
					context.write(new Text(fields[0]), new Text(s2.hero_id+","+win+",0"));
					context.write(new Text(fields[0]), new Text(s3.hero_id+","+win+",0"));
					context.write(new Text(fields[0]), new Text(s4.hero_id+","+win+",0"));
                    // Dire
                    win = "true";
                    if(fields[2].equals("t")){ // flip win for dire slots
                        win = "false";
					}
					context.write(new Text(fields[0]), new Text(s5.hero_id+","+win+",1"));
					context.write(new Text(fields[0]), new Text(s6.hero_id+","+win+",1"));
					context.write(new Text(fields[0]), new Text(s7.hero_id+","+win+",1"));
					context.write(new Text(fields[0]), new Text(s8.hero_id+","+win+",1"));
					context.write(new Text(fields[0]), new Text(s9.hero_id+","+win+",1"));
                }   
            }
}
class Players {
    JsonElement slot0;
    JsonElement slot1;
    JsonElement slot2;
    JsonElement slot3;
    JsonElement slot4;
    JsonElement slot5;
    JsonElement slot6;
    JsonElement slot7;
    JsonElement slot8;
    JsonElement slot9;
}

class Slot {
    String hero_id;
}