import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.*;

public class profileReducer
    extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
        
        int count = 0;
        int zero_duration = 0;
        int radiant_win = 0;
        int radiant_lose = 0;
        int max_duration = 0;
        int min_duration = Integer.MAX_VALUE;
        int max_tower_r = 0;
        int min_tower_r = 15;
        int max_tower_d = 0;
        int min_tower_d = 15;
        int max_barrack_r = 0;
        int min_barrack_r = 7;
        int max_barrack_d = 0;
        int min_barrack_d = 7;
        int max_fb = 0;
        int min_fb = Integer.MAX_VALUE;
        int max_humanplayer = 0;
        int min_humanplayer = 10;
        for(Text t: values){
            count++;
            String line = t.toString();
            if(line.equals("")){
                zero_duration++;
                continue;
            }
            String[] tokens = line.split(",");
            radiant_win += Integer.valueOf(tokens[1]);
            radiant_lose += Integer.valueOf(tokens[2]);
            max_duration = Math.max(max_duration, Integer.valueOf(tokens[3]));
            min_duration = Math.min(min_duration, Integer.valueOf(tokens[3]));
            max_tower_r = Math.max(max_tower_r, Integer.valueOf(tokens[4]));
            min_tower_r = Math.min(min_tower_r, Integer.valueOf(tokens[4]));
            max_tower_d = Math.max(max_tower_d, Integer.valueOf(tokens[5]));
            min_tower_d = Math.min(min_tower_d, Integer.valueOf(tokens[5]));
            max_barrack_r = Math.max(max_barrack_r, Integer.valueOf(tokens[6]));
            min_barrack_r = Math.min(min_barrack_r, Integer.valueOf(tokens[6]));
            max_barrack_d = Math.max(max_barrack_d, Integer.valueOf(tokens[7]));
            min_barrack_d = Math.min(min_barrack_d, Integer.valueOf(tokens[7]));
            max_fb = Math.max(max_fb, Integer.valueOf(tokens[8]));
            min_fb = Math.min(min_fb, Integer.valueOf(tokens[8]));
            max_humanplayer = Math.max(max_humanplayer, Integer.valueOf(tokens[9]));
            min_humanplayer = Math.min(min_humanplayer, Integer.valueOf(tokens[9]));
        }
        String res ="\nTotal count: " + String.valueOf(count) + 
                    "\nDuration<60s count: " + String.valueOf(zero_duration) + 
                    "\nRadiant win: " + String.valueOf(radiant_win) +
                    "\nRadiant lose: " + String.valueOf(radiant_lose) +
                    "\nMax duration: " + String.valueOf(max_duration) + ", Min duration: " + String.valueOf(min_duration) +
                    "\nMax tower radiant: " + String.valueOf(max_tower_r) + ", Min tower radient: " + String.valueOf(min_tower_r) +
                    "\nMax tower dire: " + String.valueOf(max_tower_d) + ", Min tower dire: " + String.valueOf(min_tower_d) +
                    "\nMax barrack radiant: " + String.valueOf(max_barrack_r) + ", Min tower radient: " + String.valueOf(min_barrack_r) +
                    "\nMax barrack dire: " + String.valueOf(max_barrack_r) + ", Min tower dire: " + String.valueOf(min_barrack_d) +
                    "\nMax first blood: " + String.valueOf(max_fb) + ", Min first blood: " + String.valueOf(min_fb) +
                    "\nMax human player: " + String.valueOf(max_humanplayer) + ", Min human player: " + String.valueOf(min_humanplayer);

        context.write(key,new Text(res)); 
            
    }
}

/*String res = "id,"+  // count
                String.valueOf(win) +","+ // win count
                String.valueOf(lose) +","+ // lose count
                fd[1] +","+ // duration
                String.valueOf(tower_status_r) +","+ 
                String.valueOf(tower_status_d) +","+ 
                String.valueOf(barrack_status_r) +","+ 
                String.valueOf(barrack_status_d) +","+ 
                fd[6] +","+ // first blood
                fd[7] +","+ // human_players
                fd[8]; // game_mode*/