package org.abc.dash;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import com.follett.fsc.core.k12.beans.X2BaseBean;

/**
 * This simple cache keeps a WeakReference to beans.
 */
public class WeakReferenceBeanCache {
	protected Map<Class<?>, Map<String, WeakReference<X2BaseBean>>> weakRefsByBeanType = new HashMap<>();
	protected Map<Class<?>, ReferenceQueue<X2BaseBean>> refQueuesByBeanType = new HashMap<>();
	
	/**
	 * Return a bean based on its class and oid, or return null.
	 * 
	 * @param beanType the optional bean type. This is highly recommended, but it will be automatically resolved based on the oid if left null.
	 * @param oid the bean oid to look up. If this is null then this method immediately returns null.
	 * @return the bean that matches the oid argument, or null.
	 */
	public X2BaseBean getBeanByOid(Class<?> beanType,String oid) {
		// normally I'd throw an IllegalArgumentException if oid is null, but X2Broker's accepted
		// convention is to allow null oids in X2Broker.getBeanByOid(), so for parity's sake we'll
		// do the same here:
		if(oid==null)
			return null;
		
		if(beanType==null)
			beanType = Dash.getBeanTypeFromOid(oid);
		
		synchronized(this) {
			purgeLostReferences();
			
			Map<String, WeakReference<X2BaseBean>> refMap = weakRefsByBeanType.get(beanType);
			if(refMap==null)
				return null;
				
			WeakReference<X2BaseBean> beanRef = refMap.get(oid);
			if(beanRef==null)
				return null;
			
			return beanRef.get();
		}
	}
	
	/**
	 * Store a WeakReference to the given bean in this cache.
	 * @param bean the bean to cache. If this is null then this method immediately returns.
	 */
	public void storeBean(X2BaseBean bean) {
		if(bean==null)
			return;

		synchronized(this) {
			purgeLostReferences();
			
			Map<String, WeakReference<X2BaseBean>> refMap = weakRefsByBeanType.get(bean.getClass());
			if(refMap==null) {
				refMap = new HashMap<>();
				weakRefsByBeanType.put(bean.getClass(), refMap);
			}
			
			ReferenceQueue<X2BaseBean> refQueue = refQueuesByBeanType.get(bean.getClass());
			if(refQueue==null) {
				refQueue = new ReferenceQueue<>();
				refQueuesByBeanType.put(bean.getClass(), refQueue);
			}
			
			refMap.put(bean.getOid(), new WeakReference<>(bean, refQueue));
		}
	}
	
	/**
	 * Clear out WeakReferences that have been enqueued.
	 */
	private void purgeLostReferences() {
		Iterator<Entry<Class<?>, ReferenceQueue<X2BaseBean>>> iter1 = refQueuesByBeanType.entrySet().iterator();
		while(iter1.hasNext()) {
			Entry<Class<?>, ReferenceQueue<X2BaseBean>> entry1 = iter1.next();
			ReferenceQueue<X2BaseBean> refQueue = entry1.getValue();
			
			int removedRefs = 0;
			while(refQueue.poll()!=null) {
				removedRefs++;
			}
			if(removedRefs>0) {
				Map<String, WeakReference<X2BaseBean>> refMap = weakRefsByBeanType.get(entry1.getKey());
				
				Iterator<Entry<String, WeakReference<X2BaseBean>>> iter2 = refMap.entrySet().iterator();
				while(iter2.hasNext() && removedRefs>0) {
					Entry<String, WeakReference<X2BaseBean>> entry2 = iter2.next();
					WeakReference<X2BaseBean> ref = entry2.getValue();
					if(ref.isEnqueued() || ref.get() == null) {
						iter2.remove();
						removedRefs--;
					}
				}
				if(refMap.isEmpty()) {
					weakRefsByBeanType.remove(entry1.getKey());
					iter1.remove();
				}
			}
		}
	}
	
	/**
	 * Clear all data from this cache.
	 */
	public void clear() {
		synchronized(this) {
			refQueuesByBeanType.clear();
			weakRefsByBeanType.clear();
		}
	}

	/**
	 * Clear all data related to a specific bean type from this cache.
	 */
	public void clear(Class<?> beanType) {
		synchronized(this) {
			refQueuesByBeanType.clear();
			weakRefsByBeanType.clear();
		}
	}
}
