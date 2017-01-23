import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AttributedEventReducer
		extends
		Reducer<AttributedEventsKey, AttributedEventsGenWritable, NullWritable,Text> {

	private Stack<AttributedEventsKey> impressionKeySt;
	private List<EventsReport> eventsReportLst;
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    
	@Override
	protected void cleanup(
			Reducer<AttributedEventsKey, AttributedEventsGenWritable,NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (EventsReport rept : eventsReportLst) {
			StringBuilder strBuilder = new StringBuilder(" Advertiser"
					+ rept.getAdvertiserId());
			strBuilder.append(" : ");
			strBuilder.append(rept.getEventCount());
			strBuilder.append(" attributed event, ");
			strBuilder.append(rept.getUserId().size());
			if (rept.getUserId().size() > 0) {
				strBuilder.append(" unique user ");
			} else {
				strBuilder
						.append(" unique user that has generated an attributed");
			}
			multipleOutputs.write( NullWritable.get(), new Text(strBuilder.toString()),"statitics");
		}
		impressionKeySt.clear();
		eventsReportLst.clear();
		multipleOutputs.close();
		super.cleanup(context);
	}

	@Override
	protected void setup(
			Reducer<AttributedEventsKey, AttributedEventsGenWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		eventsReportLst = new ArrayList<EventsReport>();
		impressionKeySt = new Stack<AttributedEventsKey>();
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		super.setup(context);
	}

	private boolean isAttributed(AttributedEventsKey aKey) {
		try {
			if (impressionKeySt.isEmpty() == false) {
				AttributedEventsKey impressionKey = impressionKeySt.pop();
				if (impressionKey != null) {
					if (impressionKey.getTimestamp().compareTo(
							aKey.getTimestamp()) <= 0)
						return true;
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return false;
	}

	@Override
	protected void reduce(
			AttributedEventsKey key,
			Iterable<AttributedEventsGenWritable> values,
			Reducer<AttributedEventsKey, AttributedEventsGenWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (AttributedEventsGenWritable value : values) {
			Writable aWritable = value.get();
			if (aWritable instanceof IntWritable) {
				if (isAttributed(key)) {
					sum += ((IntWritable) aWritable).get();
					addReport(key, sum);

				} else {
					addAdvertiser(key);
				}
			} else {
				impressionKeySt.push(key);
			}
		}
	}

	private void addReport(AttributedEventsKey key, int sum) {
		String advertiserId = key.getAdvertiserId().toString();
		boolean found = false;
		for (EventsReport evReport : eventsReportLst) {
			if (evReport.getAdvertiserId().equals(advertiserId)) {
				evReport.setUserId(key.getUserId().toString());
				evReport.setEventCount(sum);
				found = true;
				break;
			}
		}
		if (!found) {
			eventsReportLst.add(new EventsReport(key.getAdvertiserId()
					.toString(), sum, key.getUserId().toString()));
		}
	}

	private void addAdvertiser(AttributedEventsKey key) {
		String advertiserId = key.getAdvertiserId().toString();
		boolean found = false;
		for (EventsReport evReport : eventsReportLst) {
			if (evReport.getAdvertiserId().equals(advertiserId)) {
				found = true;
				break;
			}
		}
		if (!found) {
			eventsReportLst.add(new EventsReport(key.getAdvertiserId()
					.toString()));
		}
	}
}
