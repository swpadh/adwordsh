import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class AttributedEventsKeyPartitioner extends
		Partitioner<AttributedEventsKey, IntWritable> {

	@Override
	public int getPartition(AttributedEventsKey key, IntWritable value,
			int numPartition) {
		return Math.abs(key.hashCode() * 31) % numPartition;
	}

}
