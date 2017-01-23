import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AttributedEventsKey implements
		WritableComparable<AttributedEventsKey> {
	LongWritable timestamp;
	Text userId;
	Text advertiserId;
	Text eventType;
	
	
	public AttributedEventsKey() {
		timestamp = new LongWritable();
		userId = new Text();
		advertiserId = new Text();
		eventType = new Text();
		
	}

	public AttributedEventsKey(String timestamp, String userId, String advertiserId,
			String eventType) {
		this.timestamp = new LongWritable(Long.parseLong(timestamp));	
		this.userId = new Text(userId);
		this.advertiserId = new Text(advertiserId);
		this.eventType = new Text(eventType);
		
	}

	@Override
	public void readFields(DataInput dataIn) throws IOException {
		timestamp.readFields(dataIn);
		userId.readFields(dataIn);
		advertiserId.readFields(dataIn);
		eventType.readFields(dataIn);
		
	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		timestamp.write(dataOut);
		userId.write(dataOut);
		advertiserId.write(dataOut);
		eventType.write(dataOut);	
	}

	@Override
	public int compareTo(AttributedEventsKey key) {
		int cmp = timestamp.compareTo(key.timestamp);
		if (cmp != 0) {
			return cmp;
		}
		cmp = userId.compareTo(key.userId);
		if (cmp != 0) {
			return cmp;
		}
		cmp = advertiserId.compareTo(key.advertiserId);
		if (cmp != 0) {
			return cmp;
		}
		cmp = eventType.compareTo(key.eventType);
		return cmp;
	}

	public static int compare(AttributedEventsKey key1, AttributedEventsKey key2) {
		int cmp = key1.timestamp.compareTo(key2.timestamp);
		if (cmp != 0) {
			return cmp;
		}
		cmp = key1.userId.compareTo(key2.userId);
		if (cmp != 0) {
			return cmp;
		}
		cmp = key1.advertiserId.compareTo(key2.advertiserId);
		if (cmp != 0) {
			return cmp;
		}
		cmp = key1.eventType.compareTo(key2.eventType);	
		return cmp;
	}

	public LongWritable getTimestamp() {
		return timestamp;
	}
	public Text getUserId() {
		return userId;
	}

	public Text getAdvertiserId() {
		return advertiserId;
	}

	public Text getEventType() {
		return eventType;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((advertiserId == null) ? 0 : advertiserId.hashCode());
		result = prime * result + ((userId == null) ? 0 : userId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AttributedEventsKey other = (AttributedEventsKey) obj;
		if (advertiserId == null) {
			if (other.advertiserId != null)
				return false;
		} else if (!advertiserId.equals(other.advertiserId))
			return false;
		if (eventType == null) {
			if (other.eventType != null)
				return false;
		} else if (!eventType.equals(other.eventType))
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		if (userId == null) {
			if (other.userId != null)
				return false;
		} else if (!userId.equals(other.userId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "AttributedEventsKey [userId=" + userId + ", advertiserId="
				+ advertiserId + ", eventType=" + eventType + ", timestamp="
				+ timestamp + "]";
	}

}
