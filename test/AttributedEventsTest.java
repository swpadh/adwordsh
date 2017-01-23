import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ EventsMapper.class, ImpressionsMapper.class,
		AttributedEventReducer.class })
public class AttributedEventsTest {
	MapDriver<LongWritable, Text, AttributedEventsKey, AttributedEventsGenWritable> eventsMapDriver;
	MapDriver<LongWritable, Text, AttributedEventsKey, AttributedEventsGenWritable> impressionsMapDriver;
	ReduceDriver<AttributedEventsKey, AttributedEventsGenWritable, NullWritable, Text> reduceDriver;
	MultipleInputsMapReduceDriver<AttributedEventsKey, AttributedEventsGenWritable, NullWritable, Text> mapReduceDriver;
	
	@Before
	public void setUp() {
		EventsMapper eventsMapper = new EventsMapper();
		ImpressionsMapper impressionsMapper = new ImpressionsMapper();

		AttributedEventReducer reducer = new AttributedEventReducer();
		eventsMapDriver = MapDriver.newMapDriver(eventsMapper);
		impressionsMapDriver = MapDriver.newMapDriver(impressionsMapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MultipleInputsMapReduceDriver
				.newMultipleInputMapReduceDriver(reducer);
		//LogFactory.getFactory()..getRootLogger().setLevel(Level.DEBUG);
	}

	@Ignore
	public void testEventsMapper() {
		try {
			eventsMapDriver
					.withInput(
							new LongWritable(),
							new Text(
									"1450631448,325f8cc6-c4ae-42ad-895f-09fd231a8d79,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5,event"));
			AttributedEventsKey key = new AttributedEventsKey("1450631448",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "1", "event");
			eventsMapDriver.withOutput(key, new AttributedEventsGenWritable(
					new IntWritable(1)));
			eventsMapDriver
					.withInput(
							new LongWritable(),
							new Text(
									"1450631452,296f20ed-7e90-4c47-aed6-ce1a548cfd3a,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5,event"));
			key = new AttributedEventsKey("1450631452",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "1", "event");
			eventsMapDriver.withOutput(key, new AttributedEventsGenWritable(
					new IntWritable(1)));
			eventsMapDriver
					.withInput(
							new LongWritable(),
							new Text(
									"1450631464,713bcc89-1c7b-4d0e-b2d3-83bdaaf0b99d,1,16340204-80e3-411f-82a1-e154c0845cae,event"));
			key = new AttributedEventsKey("1450631464",
					"16340204-80e3-411f-82a1-e154c0845cae", "1", "event");
			eventsMapDriver.withOutput(key, new AttributedEventsGenWritable(
					new IntWritable(1)));

			eventsMapDriver
					.withInput(
							new LongWritable(),
							new Text(
									"1450631466,06d93794-9b36-4e4d-ba01-ac58aa087e02,2,60b74052-fd7e-48e4-aa61-3c14c9c714d5,event"));
			key = new AttributedEventsKey("1450631466",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "2", "event");
			eventsMapDriver.withOutput(key, new AttributedEventsGenWritable(
					new IntWritable(1)));

			eventsMapDriver
					.withInput(
							new LongWritable(),
							new Text(
									"1450631468,12494398-5b16-4971-be26-df6da0d984df,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5,event"));
			key = new AttributedEventsKey("1450631468",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "1", "event");
			eventsMapDriver.withOutput(key, new AttributedEventsGenWritable(
					new IntWritable(1)));

			eventsMapDriver.runTest();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Ignore
	public void testImpressionsMapper() {
		try {
			impressionsMapDriver.withInput(new LongWritable(), new Text(
					"1450631450,1,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5"));
			AttributedEventsKey key = new AttributedEventsKey("1450631450",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "1", "A");
			impressionsMapDriver
					.withOutput(
							key,
							new AttributedEventsGenWritable(
									new Text(
											"1450631450,1,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5")));
			impressionsMapDriver.runTest();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Ignore
	public void testReducer() {
		try {
			List<AttributedEventsGenWritable> values = new ArrayList<AttributedEventsGenWritable>();
			values.add(new AttributedEventsGenWritable(new IntWritable(1)));
			AttributedEventsKey key = new AttributedEventsKey("1450631448",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "1", "event");
			reduceDriver.withInput(key, values);
			key = new AttributedEventsKey("1450631450",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "1", "A");
			values = new ArrayList<AttributedEventsGenWritable>();
			values.add(new AttributedEventsGenWritable(new Text(
					"1450631450,1,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5")));
			reduceDriver.withInput(key, values);
			key = new AttributedEventsKey("1450631452",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "1", "event");
			values = new ArrayList<AttributedEventsGenWritable>();
			values.add(new AttributedEventsGenWritable(new IntWritable(1)));
			reduceDriver.withInput(key, values);
			key = new AttributedEventsKey("1450631464",
					"16340204-80e3-411f-82a1-e154c0845cae", "1", "event");
			reduceDriver.withInput(key, values);
			key = new AttributedEventsKey("1450631466",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "2", "event");
			reduceDriver.withInput(key, values);
			key = new AttributedEventsKey("1450631468",
					"60b74052-fd7e-48e4-aa61-3c14c9c714d5", "1", "event");
			reduceDriver.withInput(key, values);

			reduceDriver
					.withPathOutput(
							NullWritable.get(),
							new Text(
									" Advertiser-1 : 1 attributed event, 1 unique user "),
							"statistics")
					.withPathOutput(
							NullWritable.get(),
							new Text(
									" Advertiser-2 : 0 attributed event, 0 unique user that has generated an attributed"),
							"statistics");
			Configuration conf = reduceDriver.getConfiguration();
			conf.set("mapreduce.output.fileoutputformat.outputdir",
					"statistics");

			reduceDriver.runTest(false);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Test
	public void testMapReduce() {
		try {
			EventsMapper eventsMapper = new EventsMapper();
			ImpressionsMapper impressionsMapper = new ImpressionsMapper();
			mapReduceDriver.addMapper(eventsMapper);
			mapReduceDriver.addMapper(impressionsMapper);
			mapReduceDriver
					.withInput(
							eventsMapper,
							new LongWritable(),
							new Text(
									"1450631448,325f8cc6-c4ae-42ad-895f-09fd231a8d79,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5,event"));
			mapReduceDriver
					.withInput(
							impressionsMapper,
							new LongWritable(),
							new Text(
									"1450631450,1,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5"));
			mapReduceDriver
					.withInput(
							eventsMapper,
							new LongWritable(),
							new Text(
									"1450631452,296f20ed-7e90-4c47-aed6-ce1a548cfd3a,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5,event"));
			mapReduceDriver
					.withInput(
							eventsMapper,
							new LongWritable(),
							new Text(
									"1450631464,713bcc89-1c7b-4d0e-b2d3-83bdaaf0b99d,1,16340204-80e3-411f-82a1-e154c0845cae,event"));
			mapReduceDriver
					.withInput(
							eventsMapper,
							new LongWritable(),
							new Text(
									"1450631466,06d93794-9b36-4e4d-ba01-ac58aa087e02,2,60b74052-fd7e-48e4-aa61-3c14c9c714d5,event"));
			mapReduceDriver
					.withInput(
							eventsMapper,
							new LongWritable(),
							new Text(
									"1450631468,12494398-5b16-4971-be26-df6da0d984df,1,60b74052-fd7e-48e4-aa61-3c14c9c714d5,event"));
			mapReduceDriver.withPathOutput(
							NullWritable.get(),
							new Text(
									" Advertiser-1 : 1 attributed event, 1 unique user "),
							"statistics")
					.withPathOutput(
							NullWritable.get(),
							new Text(
									" Advertiser-2 : 0 attributed event, 0 unique user that has generated an attributed"),
							"statistics");
			mapReduceDriver
					.withKeyGroupingComparator(new AttributedEventsKeyGroupComparator());
			mapReduceDriver
					.withKeyOrderComparator(new AttributedEventsKeySortComparator());
			Configuration conf = mapReduceDriver.getConfiguration();
			conf.set("mapreduce.output.fileoutputformat.outputdir",
					"statistics");

			List<Pair<NullWritable, Text>> out = mapReduceDriver.run();
			
			final List<Pair<NullWritable, Text>> expected = new ArrayList<Pair<NullWritable, Text>>();
		    expected.add(new Pair<NullWritable, Text>(NullWritable.get(),
					new Text(
							" Advertiser-1 : 1 attributed event, 1 unique user ")));
		    expected.add(new Pair<NullWritable, Text>(NullWritable.get(),
					new Text(
							" Advertiser-2 : 0 attributed event, 0 unique user that has generated an attributed")));
		    //assertListEquals(expected, out);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	public static void assertListEquals(List expected, List actual) {
	    if (expected.size() != actual.size()) {
	      fail("Expected list of size " + expected.size() + "; actual size is "
	          + actual.size());
	    }

	    for (int i = 0; i < expected.size(); i++) {
	      Object t1 = expected.get(i);
	      Object t2 = actual.get(i);

	      if (!t1.equals(t2)) {
	        fail("Expected element " + t1 + " at index " + i
	            + " != actual element " + t2);
	      }
	    }
	  }
	
}