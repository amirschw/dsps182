import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperClass extends Mapper<Text, Text, BigramKey, NGramValue> {

	@Override
	public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
        // key: <w1 w2 decade>
        // value: occurrences

		String [] valueWords = value.toString().split(" ");
		String [] keyWords = key.toString().split(" ");
		IntWritable decade = new IntWritable(Integer.parseInt(keyWords[0]));
        BigramKey key1;
		NGramValue newVal;

		if (keyWords[2].equals("*")) { // total for decade OR occurrences for 1-gram
            newVal = new NGramValue(new IntWritable(Integer.parseInt(valueWords[0])));
            key1 = new BigramKey(new Text(keyWords[1]), new Text(keyWords[2]), new Text(keyWords[1]), decade);
            context.write(key1, newVal);
        }
		else { // occurrences for 2-gram
			newVal=new NGramValue(
					new IntWritable(Integer.parseInt(valueWords[0])),
                    new IntWritable(Integer.parseInt(valueWords[1])));
			key1 = new BigramKey(new Text(keyWords[1]), new Text(keyWords[2]), new Text(keyWords[2]), decade);
			context.write(key1, newVal);
		}
	}
}