import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<BigramKey, NGramValue> {
	
	// ensure that keys with same decade are directed to the same reducer
	@Override
	public int getPartition(BigramKey key,NGramValue value, int numPartitions) {
		return  Math.abs(key.getDecade().toString().hashCode()) % numPartitions;
	}
}