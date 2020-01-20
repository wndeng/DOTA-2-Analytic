import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.BooleanWritable;

public class Clean extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		Configuration c = getConf();
		Job job = Job.getInstance(c, "Clean");
		job.setJarByClass(Clean.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CleanMapper.class);
		job.setReducerClass(CleanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Input Error");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Clean(), args);
		System.exit(res);
	}
}