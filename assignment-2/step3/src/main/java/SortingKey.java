import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SortingKey implements WritableComparable<SortingKey> {
    private String decade, word1, word2;
    private double likelihood;

    public SortingKey() {
        this.decade = "";
        this.word1 = "";
        this.word2 = "";
        this.likelihood = -1;
    }

    public SortingKey(String decade, String word1, String word2,double likelihood) {
        this.decade = decade;
        this.word1 = word1;
        this.word2 = word2;
        this.likelihood = likelihood;
    }

    public void readFields(DataInput in) throws IOException {
        this.decade = in.readUTF();
        this.word1 = in.readUTF();
        this.word2 = in.readUTF();
        this.likelihood = in.readDouble();

    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.decade);
        out.writeUTF(this.word1);
        out.writeUTF(this.word2);
        out.writeDouble(this.likelihood);
    }

    public int compareTo(SortingKey o) {
        // sort by decade and then by likelihood
        int ret = this.decade.compareTo(o.decade);
        if (ret == 0) {
            double check;

            if (Double.isNaN(this.likelihood))
                check = 1;
            else if (Double.isNaN(o.likelihood))
                check = -1;
            else
                check = o.likelihood - this.likelihood;

            if (check > 0)
                ret = 1;
            else if (check < 0)
                ret = -1;
            else {
                ret = this.word1.compareTo(o.word1);
                if (ret == 0)
                    ret = this.word2.compareTo(o.word2);
            }
        }
        return ret;
    }

    public String getWords() {
        return this.decade+" "+ this.word1+ " " + this.word2 + " ";
    }

    public double getLikelihood() {
        return likelihood;
    }

    public String getDecade(){
        return this.decade;
    }

}