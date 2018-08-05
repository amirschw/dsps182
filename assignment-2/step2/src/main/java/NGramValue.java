import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class NGramValue implements Writable {
	private IntWritable occurrences;
	private IntWritable occurrencesOfW1;

	public NGramValue(IntWritable occurrences, IntWritable occurrencesOfW1) {
		this.occurrences = occurrences;
		this.occurrencesOfW1 = occurrencesOfW1;
	}

    public NGramValue(IntWritable occurrences) {
        this(occurrences, new IntWritable(1));
    }

    public NGramValue() {
	    this(new IntWritable(-1), new IntWritable(1));
    }

    public IntWritable getOccurrences() {
		return occurrences;
	}

	public IntWritable getOccurrencesOfW1() {
		return this.occurrencesOfW1;
	}
	
	public void readFields(DataInput in) throws IOException {
	    this.occurrences.readFields(in);
	    this.occurrencesOfW1.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
	    this.occurrences.write(out);
	    this.occurrencesOfW1.write(out);
	}

}
