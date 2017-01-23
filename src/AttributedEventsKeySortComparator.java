import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AttributedEventsKeySortComparator extends WritableComparator {

	public AttributedEventsKeySortComparator() {
		super(AttributedEventsKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		AttributedEventsKey key1 = (AttributedEventsKey) a;
		AttributedEventsKey key2 = (AttributedEventsKey) b;
		return AttributedEventsKey.compare(key1, key2);
	}
}
