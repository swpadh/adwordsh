import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImpressionsMapper extends
		Mapper<LongWritable, Text, AttributedEventsKey, AttributedEventsGenWritable> {
	Text value = new Text("inpressions");

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, AttributedEventsKey, AttributedEventsGenWritable>.Context context)
			throws IOException, InterruptedException {
		String[] strEvents = value.toString().split(",");
		if (strEvents.length >= 4) {	
			AttributedEventsKey aKey = new AttributedEventsKey(strEvents[0], strEvents[3],
					strEvents[1], "A");
			context.write(aKey, new AttributedEventsGenWritable(value));
		}
	}
}
