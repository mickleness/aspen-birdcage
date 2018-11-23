package org.abc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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
		RandomSample ideal = new RandomSample(0) {

			@Override
			public <T> Collection<T> create(Iterator<T> iter, int size) {
				List<T> c = new ArrayList<>();
				while(iter.hasNext()) {
					c.add(iter.next());
				}
				Collections.shuffle(c, random);
				return c.subList(0, size);
			}
			
		};
		
		List<Integer> elements = new ArrayList<>();
		for(int a = 0; a<20; a++) {
			elements.add(a);
		}
		
		Map<Integer, AtomicInteger> idealFrequencyMap = createFrequencyMap(ideal, elements);
		double idealStdDev = getStandardDeviation(idealFrequencyMap.values());
		
		Map<Integer, AtomicInteger> actualFrequencyMap = createFrequencyMap(new RandomSample(), elements);
		double actualStdDev = getStandardDeviation(actualFrequencyMap.values());
		
		assertTrue("the actual standard deviation ("+actualStdDev+") should be very similar to the ideal standard deviation ("+idealStdDev+")", actualStdDev < idealStdDev * 1.25);
	
		int avg = getSum(actualFrequencyMap.values()) / actualFrequencyMap.size();

		for(Entry<Integer, AtomicInteger> entry : actualFrequencyMap.entrySet()) {
			int diff = Math.abs(avg - entry.getValue().intValue());
			assertTrue("the value "+entry.getKey()+" is too far from the average "+avg, diff < actualStdDev*2);
		}
	}
	
	private int getSum(Collection<AtomicInteger> values) {
		int sum = 0;
		for(AtomicInteger i : values) {
			sum += i.intValue();
		}
		return sum;
	}

	private Map<Integer, AtomicInteger> createFrequencyMap(RandomSample randomSample,List<Integer> elements) {
		Map<Integer, AtomicInteger> frequencyMap = new TreeMap<>();
		for(int a = 0; a<10000; a++) {
			Collection<Integer> samples = randomSample.create(elements.iterator(), 10);
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

	private double getStandardDeviation(Collection<AtomicInteger> values) {
		double avg = 0;
		for(Number v : values) {
			avg += v.doubleValue();
		}
		avg = avg / values.size();
		
		double sigmaSquared = 0;
		for(Number v : values) {
			sigmaSquared += (v.doubleValue() - avg) * (v.doubleValue() - avg);
		}
		sigmaSquared = sigmaSquared / values.size();
		return Math.sqrt(sigmaSquared);
	}
}
