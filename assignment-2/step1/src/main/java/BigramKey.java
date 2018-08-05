import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class BigramKey implements WritableComparable<BigramKey>{
	
	private Text word1;
	private Text word2;
	private IntWritable	decade;

	public BigramKey(Text word1, Text word2, IntWritable decade) {
		this.word1 = word1;
		this.word2 = word2;
        this.decade = decade;
	}

	public BigramKey(IntWritable decade) {
		this(new Text("*"),new Text("*"),decade);
	}

	public BigramKey() {
        this(new Text("*"),new Text("*"), new IntWritable(-1));
	}

	public void readFields(DataInput in) throws IOException {
		word1.readFields(in);
		word2.readFields(in);
        decade.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		word1.write(out);
		word2.write(out);
		decade.write(out);
	}

	public int compareTo(BigramKey other) {
		// if this.decade == other.decade, put <decade * *> first
		// if this.w1 == other.w1, put <w1, *> before <w1,w2>
		int ret = this.decade.get() - other.decade.get();
		if (ret == 0) {
			if (this.word1.toString().equals("*") && !other.word1.toString().equals("*"))
				ret = -1;
			else if (other.word1.toString().equals("*") && !this.word1.toString().equals("*"))
				ret = 1;
			else
				ret = this.word1.toString().compareTo(other.word1.toString());
		}
		if (ret == 0) { //same decade and same w1
			if (this.word2.toString().equals("*") && !other.word2.toString().equals("*"))
				ret = -1;
			else if (other.word2.toString().equals("*") &&  !this.word2.toString().equals("*"))
				ret = 1;
			else
				ret = this.word2.toString().compareTo(other.word2.toString());
		}
		return ret;
	}
	

	public IntWritable getDecade() {
		return this.decade;
	}

	public Text getWord1() {
		return this.word1;
	}
	
	public Text getWord2() {
		return this.word2;
	}
	
	
	@Override
	public String toString() {
		return this.decade + " " + this.word1 + " " + this.word2;
	}
}
