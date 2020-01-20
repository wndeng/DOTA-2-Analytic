import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.Integer;

public class profileMapper
extends Mapper<LongWritable, Text, Text, Text> {
    private static final int MISSING = 9999;

@Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] tokens = line.split("\t");
        String fields = tokens[1];
        String[] fd = fields.split(",");
        int win = 0, lose = 0;
        if(fd[0].equals("t")){
            win = 1;
        }else{
            lose = 1;
        }
        int tower_status_r = Integer.bitCount(Integer.valueOf(fd[2]));
        int tower_status_d = Integer.bitCount(Integer.valueOf(fd[3]));
        int barrack_status_r = Integer.bitCount(Integer.valueOf(fd[4]));
        int barrack_status_d = Integer.bitCount(Integer.valueOf(fd[5]));
        
        String res = tokens[0]+","+  // count
                String.valueOf(win) +","+ // win count
                String.valueOf(lose) +","+ // lose count
                fd[1] +","+ // duration
                String.valueOf(tower_status_r) +","+ 
                String.valueOf(tower_status_d) +","+ 
                String.valueOf(barrack_status_r) +","+ 
                String.valueOf(barrack_status_d) +","+ 
                fd[6] +","+ // first blood
                fd[7] +","+ // human_players
                fd[8]; // game_mode
        if(Integer.valueOf(fd[1]) >= 180){
            context.write(new Text(""), new Text(res));
        }else{
            context.write(new Text(""), new Text(""));
        }
        
    }  
}

/*tmp = tokens[2] +","+  // radiant win
        tokens[4] +","+ // duration
        tokens[5] +","+ // tower_status_radiant
        tokens[6] +","+ // tower_status_dire
        tokens[7] +","+ // barrack_status_radiant
        tokens[8] +","+ // barrack_status_dire
        tokens[10] +","+ //first_blood_time
        tokens[12] +","+ // human_players
        tokens[16]; // game_mode*/
