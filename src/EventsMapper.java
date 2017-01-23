import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EventsMapper extends
		Mapper<LongWritable, Text, AttributedEventsKey, AttributedEventsGenWritable> {
	IntWritable count = new IntWritable(1);

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, AttributedEventsKey, AttributedEventsGenWritable>.Context context)
			throws IOException, InterruptedException {
		String[] strEvents = value.toString().split(",");
		if (strEvents.length >= 5) {			
			AttributedEventsKey aKey = new AttributedEventsKey(strEvents[0], strEvents[3],
					strEvents[2], strEvents[4]);
			context.write(aKey, new AttributedEventsGenWritable(count));
		}
	}
}
