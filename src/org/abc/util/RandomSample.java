package org.abc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * This helps collect a random subset of a large iterator.
 * <p>
 * For example: suppose you have an iterator that will iterate over
 * one million strings, and you want to randomly collect 100 of them.
 */
public class RandomSample {
	
	protected final Random random;

	/**
	 * Create a RandomSample with a dynamic random seed.
	 */
	public RandomSample() {
		this(System.currentTimeMillis());
	}
	
	/**
	 * Create a RandomSample with a specific random seed.
	 */
	public RandomSample(long randomSeed) {
		this.random = new Random(randomSeed);
	}
	
	private static class Entry<T> {
		double ranking;
		T value;
	}
	
	/**
	 * Iterate over an unknown number of elements and create a
	 * subset of a fixed size.
	 * <p>
	 * An inefficient implementation of this method would resemble:
	 * <code>
	 * List&lt;T> c = new ArrayList&lt;>();
	 * while(iter.hasNext()) {
	 * 	c.add(iter.next());
	 * }
	 * Collections.shuffle(c, random);
	 * return c.subList(0, size);
	 * </code>
	 * <p>
	 * However since the iterator may return millions of elements: it's a
	 * waste of memory to store all the elements in a list. The current
	 * implementation only stores at most size-many elements.
	 * 
	 * @param iter an iterator of an unknown number of elements.
	 * @param size the number of elements to collect in our subset.
	 * @return a subset of data in the iterator.
	 */
	public <T> Collection<T> create(Iterator<T> iter,int size) {
		@SuppressWarnings("unchecked")
		Entry<T>[] entries = new Entry[size];
		int index = 0;
		int entryCount = 0;
		while(iter.hasNext()) {
			T e = iter.next();
			double rank = random.nextDouble();
			if(entries[index]==null) {
				entries[index] = new Entry<>();
				entries[index].ranking = rank;
				entries[index].value = e;
				entryCount++;
			} else if(rank > entries[index].ranking) {
				entries[index].ranking = rank;
				entries[index].value = e;
			}
			index = (index+1)%size;
		}
		
		List<T> returnValue = new ArrayList<>(entryCount);
		for(int a = 0; a<entries.length; a++) {
			if(entries[a]!=null) {
				returnValue.add(entries[a].value);
				entries[a] = null;
			}
		}
		return returnValue;
	}
}
