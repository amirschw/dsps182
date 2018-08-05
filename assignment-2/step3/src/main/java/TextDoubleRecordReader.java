import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class TextDoubleRecordReader extends RecordReader<Text, DoubleWritable> {
    private LineRecordReader reader;

    public TextDoubleRecordReader() {
        reader = new LineRecordReader();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public Text getCurrentKey() {
        return parseKey(reader.getCurrentValue().toString());
    }

    //get key from the format:
    private Text parseKey(String string) {
        return new Text(string.substring(0, string.indexOf("\t")));
    }

    @Override
    public DoubleWritable getCurrentValue() {
        return parseValue(reader.getCurrentValue().toString());
    }

    private DoubleWritable parseValue(String string) {
        String numberChars = string.substring(string.indexOf("\t")+1, string.length());
        return new DoubleWritable(Double.parseDouble(numberChars));
    }

    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        reader.initialize(split, context);

    }

    @Override
    public boolean nextKeyValue() throws IOException {
        return reader.nextKeyValue();
    }

}