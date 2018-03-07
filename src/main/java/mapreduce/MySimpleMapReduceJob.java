package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MySimpleMapReduceJob extends Configured implements Tool {

	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}

		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		@Override
		protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			// ...
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}
		
		// The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		// Make sure that the output key/value classes also match those set in your job's configuration (see below).
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// ...
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf());

		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(MySimpleMapReduceJob.class);

		// 2. Set mapper and reducer classes
		// ...

		// 3. Set input and output format, mapper output key and value classes, and final output key and value classes
		// ...

		// 4. Set input and output paths; remember, these will be HDFS paths or URLs
		// ...

		// 5. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
		// ...

		// 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		boolean succeeded = job.waitForCompletion(true);
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MySimpleMapReduceJob(), args));
	}
}
