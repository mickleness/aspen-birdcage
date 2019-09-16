package org.abc.dash;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This catalogs the number of times certain uncaching operations have been
 * invoked.
 */
public class CacheResults implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * This describes an attempt to uncache a BeanQuery.
	 */
	public enum Type {
		/**
		 * This indicates caching wasn't attempted.
		 */
		SKIP, 
		/**
		 * This indicates caching turned up an exact match and no database query was issued.
		 */
		HIT, 
		/**
		 * This indicates the Dash caching layer identified the required bean oids, and we replaced the original query with an oid-based query.
		 */
		REDUCED_QUERY_TO_OIDS,
		/**
		 * This indicates we gave up on caching because we had too many beans.
		 */
		ABORT_TOO_MANY,
		/**
		 * This indicates the cache was consulted but didn't have a match.
		 */
		MISS,
		/**
		 * This indicates we were able to partially uncache some of the required beans, and we replaced the original query to identify the remaining beans.
		 */
		REDUCED_QUERY_FROM_SPLIT, 
		/**
		 * This indicates our cache layer was able to resolve the query by splitting it and resolving its split elements.
		 */
		HIT_FROM_SPLIT;
	}
	
	protected Map<Type, AtomicLong> matches = new HashMap<>();

	/**
	 * Increment the counter for a given type of result.
	 * 
	 * @param type the type of result to increment.
	 */
	public void increment(Type type) {
		AtomicLong l;
		synchronized(matches) {
			l = matches.get(type);
			if(l==null) {
				l = new AtomicLong(0);
				matches.put(type, l);
			}
		}
		l.incrementAndGet();
		return;
	}

	@Override
	public int hashCode() {
		synchronized(matches) {
		return matches.hashCode();
		}
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof CacheResults))
			return false;
		CacheResults other = (CacheResults) obj;
		Map<Type, Long> otherData = other.getData();
		Map<Type, Long> myData = getData();
		return otherData.equals(myData);
	}

	/**
	 * Return a copy of the records in this CacheResults.
	 */
	public Map<Type, Long> getData() {
		Map<Type, Long> returnValue = new HashMap<>();
		synchronized(matches) {
			for(Entry<Type, AtomicLong> entry : matches.entrySet()) {
				returnValue.put(entry.getKey(), entry.getValue().longValue());
			}
		}
		return returnValue;
	}

	@Override
	public String toString() {
		return "CacheResults[ "+matches+" ]";
	}

	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		int version = in.readInt();
		if (version == 0) {
			matches = (Map<Type, AtomicLong>) in.readObject();
		} else {
			throw new IOException("Unsupported internal version: " + version);
		}
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(0);
		out.writeObject(matches);
	}
}
