package org.abc.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * This helps collect a random subset of a large group of elements of unknown size.
 * <p>
 * For example: suppose you have an iterator that will iterate over
 * one million strings, and you want to randomly collect 100 of them.
 * <p>
 * An inefficient implementation of this basic idea would resemble:
 * <code>
 * List&lt;T> c = new ArrayList&lt;>();
 * while(iter.hasNext()) {
 * 	c.add(iter.next());
 * }
 * Collections.shuffle(c, random);
 * return c.subList(0, size);
 * </code>
 * <p>
 * If we're iterating over 10 million elements, though, keeping all those elements in memory
 * is unnecessarily wasteful if we only want a sample of 100 elements. This class makes
 * sure we only maintain an internal list of a few elements that are randomly selected
 * from the incoming data.
 * 
 */
public class RandomSample<T> {
	
	private static class Entry<T> {
		double ranking;
		T value;
	}
	
	protected final Random random;
	protected final int sampleCount;
	protected final Entry<T>[] entries;
	private int entriesIndex = 0;
	private long totalInputCount = 0;

	/**
	 * Create a RandomSample with a dynamic random seed.
	 * 
	 * @param sampleCount the number of elements to collect in our subset.
	 */
	public RandomSample(int sampleCount) {
		this(sampleCount, System.currentTimeMillis());
	}
	
	/**
	 * Create a RandomSample with a specific random seed.
	 * 
	 * @param sampleCount the number of elements to collect in our subset.
	 * @param randomSeed the random seed used to randomize which entries get selected.
	 */
	@SuppressWarnings("unchecked")
	public RandomSample(int sampleCount,long randomSeed) {
		this.random = new Random(randomSeed);
		this.sampleCount = sampleCount;
		entries = new Entry[sampleCount];
	}
	
	/**
	 * Add all the elements in the Iterable to this RandomSample.
	 */
	public void addAll(Iterable<T> iterable) {
		Iterator<T> iter = iterable.iterator();
		while(iter.hasNext()) {
			T e = iter.next();
			add(e);
		}
	}
	
	/**
	 * Return the current subset of samples.
	 */
	public synchronized List<T> getSamples() {
		List<T> returnValue = new ArrayList<>(entriesIndex);
		for(int a = 0; a<entries.length; a++) {
			if(entries[a]!=null) {
				returnValue.add(entries[a].value);
				entries[a] = null;
			}
		}
		return returnValue;
	}
	
	/**
	 * Process an element, and optionally add it to our subset of randomly sampled elements.
	 * 
	 * @param element the element to process/add.
	 */
	public synchronized void add(T element) {
		totalInputCount++;
		double rank = random.nextDouble();
		if(entries[entriesIndex]==null) {
			entries[entriesIndex] = new Entry<>();
			entries[entriesIndex].ranking = rank;
			entries[entriesIndex].value = element;
			entriesIndex++;
		} else if(rank > entries[entriesIndex].ranking) {
			entries[entriesIndex].ranking = rank;
			entries[entriesIndex].value = element;
		}
		entriesIndex = (entriesIndex+1)%entries.length;
	}

	/**
	 * Return the number of elements that have been added by calling {@link #add(Object)}.
	 * <p>
	 * Note this may be in the millions (or higher), but the actual size of the list
	 * returned by {@link #getSample()} will be a fixed size based on the value passed
	 * to the constructor of this object.
	 */
	public long getTotalElementCount() {
		return totalInputCount;
	}

	/**
	 * The size of the list that {@link #getSamples()} will return.
	 */
	public int getSampleCount() {
		return entriesIndex;
	}
}
