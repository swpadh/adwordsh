import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AttributedEventsJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new AttributedEventsJob(), args);
		System.exit(status);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf(), " Attributed Events Job ");
		// //Path inputPath1 = new Path(
		// "hdfs://sandbox.hortonworks.com:8020/tmp/admin/data/events.csv");
		// Path inputPath2 = new Path(
		// "hdfs://sandbox.hortonworks.com:8020/tmp/admin/data/impressions.csv");
		Path inputPath1 = new Path("input/events.csv");
		Path inputPath2 = new Path("input/impressions.csv");
		Path outputPath = new Path("output");

		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath);
		}

		job.setJarByClass(getClass());
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class,
				EventsMapper.class);
		MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class,
				ImpressionsMapper.class);

		job.setMapOutputKeyClass(AttributedEventsKey.class);
		job.setMapOutputValueClass(AttributedEventsGenWritable.class);

		TextOutputFormat.setOutputPath(job, outputPath);
		job.setReducerClass(AttributedEventReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setGroupingComparatorClass(AttributedEventsKeyGroupComparator.class);
		job.setPartitionerClass(AttributedEventsKeyPartitioner.class);
		job.setSortComparatorClass(AttributedEventsKeySortComparator.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
