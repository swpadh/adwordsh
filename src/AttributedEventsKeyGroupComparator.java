import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AttributedEventsKeyGroupComparator extends WritableComparator {

	public AttributedEventsKeyGroupComparator() {
		super(AttributedEventsKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		AttributedEventsKey key1 = (AttributedEventsKey) a;
		AttributedEventsKey key2 = (AttributedEventsKey) b;
		int cmp = key1.userId.compareTo(key2.userId);
		if (cmp != 0) {
			return cmp;
		}
		cmp = key1.advertiserId.compareTo(key2.advertiserId);
		return cmp;
	}
}
