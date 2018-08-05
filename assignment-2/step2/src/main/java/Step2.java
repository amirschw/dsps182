import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "step2");

		// set main class
		job.setJarByClass(Step2.class);

		// set mapper and reducer
		job.setMapperClass(MapperClass.class);
		job.setMapOutputKeyClass(BigramKey.class);
		job.setMapOutputValueClass(NGramValue.class); 

		job.setReducerClass(ReducerClass.class);

		// set partitioner
		job.setPartitionerClass(PartitionerClass.class);

		// set input
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job,  new Path(args[0]));

		// set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// wait for completion and exit
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
