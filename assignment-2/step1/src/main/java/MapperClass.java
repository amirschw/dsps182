import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

public class MapperClass extends Mapper<LongWritable, Text, BigramKey, IntWritable> {

	private static final char FIRST_LOWER_ENG_CHAR = 'a';
	private static final char LAST_LOWER_ENG_CHAR = 'z';
	private static final char FIRST_UPPER_ENG_CHAR = 'A';
	private static final char LAST_UPPER_ENG_CHAR = 'Z';

	private static final char FIRST_HEB_CHAR = (char)1488; 
	private static final char LAST_HEB_CHAR = (char)1514;

	private static ArrayList<String> stopWords;
	private static String LANGUAGE;
	
	protected void setup(Context context) {
	    Configuration conf = context.getConfiguration();
		stopWords = new ArrayList<>(Arrays.asList(conf.getStrings(Step1.STOP_WORDS_KEY)));
		LANGUAGE = conf.get(Step1.LANGUAGE, "heb");
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {

		// value: n-gram \t year \t occurrences \t volume_count \n
		// tokenize the value using tab delimiter 
		StringTokenizer itr = new StringTokenizer(value.toString(), "\t"); 

		// parse the n-gram
		String ngram = itr.nextToken();

		// filter only ngrams that contain non-alphabetic characters
		if (!onlyLettersAndSpace(ngram)) {
			return;
		}

		// get the year
		String year = itr.nextToken();
		IntWritable decade = new IntWritable(Integer.parseInt(year.substring(0, year.length()-1) + "0"));

		// parse the occurrences
		IntWritable occurrences = new IntWritable(Integer.parseInt(itr.nextToken()));

		// split n-gram to one/two words
		String [] ngramArray = ngram.split(" ");

		if (ngramArray.length==1) { // 1-gram
			if(!stopWords.contains(ngramArray[0].toLowerCase())) {
				context.write(new BigramKey(new Text(ngramArray[0]),new Text("*"),decade), occurrences);
			}
			context.write(new BigramKey(decade), occurrences);
		} else if (ngramArray.length==2){ // 2-gram
			// check that both words are not stop words
			if(!stopWords.contains(ngramArray[0].toLowerCase()) &&
               !stopWords.contains(ngramArray[1].toLowerCase())) {
				context.write(new BigramKey(new Text(ngramArray[0]),new Text(ngramArray[1]),decade), occurrences);
			}
		}
	}

	private static boolean isLetterOrSpace(char c) {
		if (LANGUAGE.equals("heb")) {
			return ((c >= FIRST_HEB_CHAR && c <= LAST_HEB_CHAR) || c == ' ');
		}
		else {
			return ((c >= FIRST_LOWER_ENG_CHAR && c <= LAST_LOWER_ENG_CHAR) ||
			        (c >= FIRST_UPPER_ENG_CHAR && c <= LAST_UPPER_ENG_CHAR) ||
                     c == ' ');
		}
	}
	
	private static boolean onlyLettersAndSpace(String string) {
		for (int i=0; i<string.length(); i++) {
			if (!isLetterOrSpace(string.charAt(i))) {
				return false;
			}
		}
		return true;
	}
}