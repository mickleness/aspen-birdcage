package org.abc.dash;

import java.io.IOException;
import java.io.Serializable;

/**
 * This monitors a few properties about a template query.
 * <p>
 * Here a "template query" means a query stripped of specific values. For example
 * you query for records where "A==1 || B==true", then the template of that query
 * resembles "A==? || B==?". Two different queries that use the same fields but
 * different values match the same template.
 * 
 */
public class TemplateQueryProfile implements QueryIteratorDash.CloseListener, Serializable {
	private static final long serialVersionUID = 1L;

	protected int ctr = 0;
	protected int maxReturnCount = 0;
	protected double averageReturnCount = 0;
	protected CacheResults results = new CacheResults();
	
	/**
	 * The total number of times a query matching this template has been issued.
	 */
	public synchronized int getCounter() {
		return ctr;
	}
	
	/**
	 * The average number of beans a QueryIterator iterated over.
	 * <p>
	 * In most cases this is synonymous with "the number of beans a query produces",
	 * but if the iterator is abandoned prematurely then these two values may be
	 * different.
	 */
	public synchronized double getAverageReturnCount() {
		return averageReturnCount;
	}
	
	/**
	 * The maximum number of beans a QueryIterator iterated over.
	 */
	public synchronized int getMaxReturnCount() {
		return maxReturnCount;
	}
	
	/**
	 * Return CacheResults associated with this template.
	 */
	public synchronized CacheResults getResults() {
		return results;
	}

	/**
	 * This object is attached to every QueryIteratorDash a
	 * BrokerDashSharedResource produces, and when that iterator is closed
	 * this object's statistics are updated.
	 */
	@Override
	public synchronized void closedIterator(int returnCount, boolean hasNext) {
		maxReturnCount = Math.max(maxReturnCount, returnCount);
		double total = averageReturnCount * ctr;
		total += returnCount;
		ctr++;
		averageReturnCount = total / ((double)ctr);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		int version = in.readInt();
		if (version == 0) {
			ctr = in.readInt();
			maxReturnCount = in.readInt();
			averageReturnCount = in.readDouble();
			results = (CacheResults) in.readObject();
		} else {
			throw new IOException("Unsupported internal version: " + version);
		}
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(0);
		out.writeInt(ctr);
		out.writeInt(maxReturnCount);
		out.writeDouble(averageReturnCount);
		out.writeObject(results);
	}
}
