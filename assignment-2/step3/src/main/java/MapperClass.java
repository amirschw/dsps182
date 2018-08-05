import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Text, DoubleWritable, SortingKey, Text> {

    @Override
    public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {

        // split the key to 4 parts
        String [] words = key.toString().split(" ");
        String decade = words[0];
        String likelihood = words[1];
        String w1 = words[2];
        String w2 = words[3];

        SortingKey sortingKey = new SortingKey(decade, w1, w2, value.get());

        // Send to context for sorting
        context.write(sortingKey, new Text(likelihood));
    }
}