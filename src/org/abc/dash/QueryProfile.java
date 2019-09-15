package org.abc.dash;

public class QueryProfile implements QueryIteratorDash.CloseListener {
	int ctr = 0;
	int maxReturnCount = 0;
	double averageReturnCount = 0;
	
	public int getCounter() {
		return ctr;
	}
	
	public double getAverageReturnCount() {
		return averageReturnCount;
	}
	
	public int getMaxReturnCount() {
		return maxReturnCount;
	}

	@Override
	public void closedIterator(int returnCount, boolean hasNext) {
		maxReturnCount = Math.max(maxReturnCount, returnCount);
		double total = averageReturnCount * ctr;
		total += returnCount;
		ctr++;
		averageReturnCount = total / ((double)ctr);
	}
}
