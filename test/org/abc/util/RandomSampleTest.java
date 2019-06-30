package org.abc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.Test;


public class RandomSampleTest extends TestCase {
	
	/**
	 * This makes sure the RandomSample is well distributed, so if you survey 10,000
	 * you see a good random representation of all elements.
	 */
	@Test
	public void testDistribution() {
		List<Integer> elements = new ArrayList<>();
		for(int a = 0; a<100; a++) {
			elements.add(a);
		}

		final int sampleSize = 10;
		Map<Integer, AtomicInteger> actualFrequencyMap = createFrequencyMap(elements, sampleSize);
		int avg = getSum(actualFrequencyMap.values()) / actualFrequencyMap.size();

		for(Entry<Integer, AtomicInteger> entry : actualFrequencyMap.entrySet()) {
			int diff = Math.abs(avg - entry.getValue().intValue());
			assertTrue("the key/value "+entry+" is too far from the average "+avg, diff < avg * .10);
		}
	}
	
	private int getSum(Collection<AtomicInteger> values) {
		int sum = 0;
		for(AtomicInteger i : values) {
			sum += i.intValue();
		}
		return sum;
	}

	private Map<Integer, AtomicInteger> createFrequencyMap(List<Integer> elements, int sampleSize) {
		RandomSample<Integer> randomSample = new RandomSample<Integer>(sampleSize, 0);
		Map<Integer, AtomicInteger> frequencyMap = new TreeMap<>();
		for(int a = 0; a<10000; a++) {
			randomSample.addAll(elements);
			Collection<Integer> samples = randomSample.getSamples();
			for(Integer sample : samples) {
				AtomicInteger occurrence = frequencyMap.get(sample);
				if(occurrence==null) {
					occurrence = new AtomicInteger(0);
					frequencyMap.put(sample, occurrence);
				}
				occurrence.incrementAndGet();
			}
		}
		
		return frequencyMap;
	}
}
