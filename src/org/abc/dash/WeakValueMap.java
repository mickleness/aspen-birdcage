package org.abc.dash;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

/**
 * This keeps a strong reference to keys and a weak reference to values.
 * <p>
 * Every time you interact with this object it calls {@link #purge()} to
 * clean up stale references. You can also set up a timer to automatically
 * invoke purge from time to time.
 */
public class WeakValueMap<K, V> {
	static class WeakValueReference<K, V> extends WeakReference<V> {
		K key;

		public WeakValueReference(K key, V referent, ReferenceQueue<? super V> q) {
			super(referent, q);
			this.key = key;
		}
		
	}
	
	protected Map<K, WeakValueReference<K, V>> map = new HashMap<>();
	protected ReferenceQueue<? super V> queue = new ReferenceQueue<>();
	
	public V get(K key) {
		synchronized(this) {
			purge();
			WeakValueReference<K, V> ref = map.get(key);
			if(ref==null)
				return null;
			return ref.get();
		}
	}
	
	public int size() {
		synchronized(this) {
			purge();
			return map.size();
		}
	}
	
	public boolean containsKey(K key) {
		synchronized(this) {
			purge();
			WeakValueReference<K, V> ref = map.get(key);
			return ref.get()!=null;
		}
	}
	
	public void put(K key, V value) {
		synchronized(this) {
			purge();
			WeakValueReference<K, V> newRef = new WeakValueReference<>(key, value, queue);
			map.put(key, newRef);
		}
	}
	
	/**
	 * Remove invalid references from this map.
	 * 
	 * @return the number of removed references.
	 */
	public int purge() {
		synchronized(this) {
			WeakValueReference<K, V> ref = (WeakValueReference<K, V>) queue.poll();
			int ctr = 0;
			while(ref!=null) {
				map.remove(ref.key);
				ref = (WeakValueReference<K, V>) queue.poll();
				ctr++;
			}
			return ctr;
		}
	}
	
	/**
	 * Convert this map of weakly reference data to a regular java.util.Map with strong references.
	 */
	public Map<K, V> toMap() {
		synchronized(this) {
			purge();
			Map<K, V> returnValue = new HashMap<>(map.size());
			for(WeakValueReference<K, V> ref : map.values()) {
				K key = ref.key;
				V value = ref.get();
				if(value!=null)
					returnValue.put(key, value);
			}
			return returnValue;
		}
	}
	
	public void clear() {
		synchronized(this) {
			for(WeakValueReference<K, V> ref : map.values()) {
				ref.enqueue();
			}
			while(true) {
				if(queue.poll()==null)
					break;
			}
			map.clear();
			queue = new ReferenceQueue<>();
		}
	}

	@Override
	public String toString() {
		Map<K, V> map = toMap();
		return map.toString();
	}
}
