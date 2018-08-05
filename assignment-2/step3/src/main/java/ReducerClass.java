import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class ReducerClass extends Reducer<SortingKey, Text, Text,DoubleWritable> {
    private int counter;
    private String decade = "";

    @Override
    public void reduce(SortingKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        if (!decade.equals(key.getDecade())) {
            decade = key.getDecade();
            counter = 0;
        }

        if (counter < 100) {
            counter++;
            context.write(
                    new Text(counter+". "+ key.getWords()),
                    new DoubleWritable(key.getLikelihood()));
        }
    }
}