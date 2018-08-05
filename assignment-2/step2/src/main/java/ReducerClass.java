import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<BigramKey, NGramValue, Text, DoubleWritable> {

	private int decadeTotal = 0;
	private int occurrencesW2 = 0;

	@Override
	public void reduce(BigramKey key, Iterable<NGramValue> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		int occurrencesW1 = 0;
		String w1 = key.getWord1().toString();
		String w2 = key.getWord2().toString();

		for (NGramValue value : values) {
			sum += value.getOccurrences().get();
			if(!w1.equals("*") && !w2.equals("*"))
				occurrencesW1 += value.getOccurrencesOfW1().get();
		}

        if (w1.equals("*")) // decade
		    	decadeTotal = sum;
		else if (w2.equals("*")) // 1-gram
			occurrencesW2 = sum;
		else { // 2-gram
			// write the likelihood to the context: <decade likelihood w1 w2>, likelihood>
			DoubleWritable calculation = calculateLikelihood(
                            (double)sum, (double)occurrencesW1, (double)occurrencesW2, (double)decadeTotal);
			context.write(new Text(key.getDecade() + " " + calculation + " " + w1 + " " + w2 ), calculation);
		}
	}

	private DoubleWritable calculateLikelihood(double c12, double c1, double c2, double N) {
		double L1 = c12 * Math.log10(c2 / N) + (c1 - c12) * Math.log10(1 - (c2 / N));
		double L2 = (c2 - c12) * Math.log10(c2 / N) + (N + c12 - c1 - c2) * Math.log10(1 - (c2 / N));
		double L3 = c12 * Math.log10(c12 / c1) + (c1 - c12) * Math.log10(1 - (c12 / c1));
		double L4 = (c2 - c12) * Math.log10((c2 - c12) / (N - c1)) +
					(N + c12 - c1 - c2) * Math.log10(1 - ((c2 - c12) / (N - c1)));

		double formula = L1 + L2 - L3 - L4;
		return new DoubleWritable((-2)*formula);
	}
}
