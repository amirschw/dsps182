import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public  class ReducerClass extends Reducer<BigramKey,IntWritable,Text,Text> {

	private int firstWordOccurrences = 0;
	
	@Override
	public void reduce(BigramKey key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // 1-gram or decade: // <decade w1, c(w1)> or <decade, total(decade)>
        if (key.getWord2().toString().equals("*")) {
            context.write(new Text(key.toString()), new Text(""+sum));
            String word1 = key.getWord1().toString();

            if (!word1.equals("*")) { // 1-gram
                 firstWordOccurrences = sum;
            }
        }

        // 2-gram: <decade w1 w2, c(<w1,w2>) c(w1)>
        else {
            context.write(
                    new Text(key.toString()),
                    new Text(String.valueOf(sum)+ " " + String.valueOf(firstWordOccurrences)));
        }
	}
}
