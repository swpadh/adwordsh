import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class AttributedEventsGenWritable extends GenericWritable{

	private static Class CLASSES[] = new Class[]{
		IntWritable.class,
		Text.class
	};
	public AttributedEventsGenWritable()
	{
		
	}
	public AttributedEventsGenWritable(Writable value)
	{
		set(value);
	}
	@Override
	protected Class<? extends Writable>[] getTypes() {
		
		return CLASSES;
	}

}
