import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.lang.Integer; 

public class cleanMapper
extends Mapper<LongWritable, Text, Text, Text> {
    private static final int MISSING = 9999;

@Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] tokens = line.split(",");
        boolean flag = true;
        if(!tokens[2].equals("t") && !tokens[2].equals("f")){
            flag = false;
        }
        String tmp = tokens[2] +","+  // radiant win
                        tokens[4] +","+ // duration
                        tokens[5] +","+ // tower_status_radiant
                        tokens[6] +","+ // tower_status_dire
                        tokens[7] +","+ // barrack_status_radiant
                        tokens[8] +","+ // barrack_status_dire
                        tokens[10] +","+ //first_blood_time
                        tokens[12] +","+ // human_players
                        tokens[16] + "," + //
                        tokens[21] + "," + //
                        tokens[22];
        if(flag){
            context.write(new Text(tokens[0]), new Text(tmp));
        }
    }  
    public static boolean isNumeric(String str) { 
        try {  
            Double.parseDouble(str);  
            return true;
        } catch(NumberFormatException e){  
            return false;  
        }  
    }
}

