import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class Step1 {

	private static final String BUCKET_NAME = "assignment2-dsps-2";
	static final String STOP_WORDS_KEY = "stopWordsList";
    static final String LANGUAGE = "language";

	private static AmazonS3 s3client;

	public static void main(String[] args) throws Exception {

        InstanceProfileCredentialsProvider credentialsProvider =
                new InstanceProfileCredentialsProvider(false);

		// connect to S3 cloud
		s3client = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion(Regions.US_WEST_2)
				.build();

		Configuration conf = new Configuration();

		// pass language & stopWords list (as an array) to the configuration
		conf.setStrings(STOP_WORDS_KEY, downloadStopWordsToArray("stop-words/" + args[0]));
        conf.set(LANGUAGE, args[0]);

		Job job = Job.getInstance(conf, "step1");

		// set main class
		job.setJarByClass(Step1.class);

		// set mapper and reducer
		job.setMapperClass(MapperClass.class);
		job.setMapOutputKeyClass(BigramKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(ReducerClass.class);

		// set combiner if local aggregation is on
		if (args[4].equals("true")) {
            job.setCombinerClass(CombinerClass.class);
        }

        // set input
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileInputFormat.addInputPath(job,new Path(args[2]));

        // set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		// wait for completion and exit
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	// download the stop words file from s3 and parse it to array
	private static String[] downloadStopWordsToArray(String path) {
		S3Object object = s3client.getObject(new GetObjectRequest(BUCKET_NAME, path));
		ArrayList<String> stopWords = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				stopWords.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stopWords.toArray(new String[0]);
	}
}
