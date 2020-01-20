import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.Math;

public class ProfileReducer
	extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	 	throws IOException, InterruptedException {
	 	Integer min_ = Integer.MAX_VALUE;
	 	Integer max_ = Integer.MIN_VALUE;
	 	for (Text value : values) {
			Integer v = Integer.valueOf(value.toString());
			min_ = Math.min(min_, v);
			max_ = Math.max(max_, v);

		}
		context.write(key, new Text("min: " + String.valueOf(min_) + ", " + "max: " + String.valueOf(max_)));
	}
}