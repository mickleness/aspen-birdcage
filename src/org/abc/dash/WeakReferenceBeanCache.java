package org.abc.dash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.abc.dash.WeakValueMap.PurgeListener;

import com.follett.fsc.core.k12.beans.X2BaseBean;

/**
 * This simple cache keeps a WeakReference to beans.
 */
public class WeakReferenceBeanCache {
	
	protected Map<Class<?>, WeakValueMap<String, X2BaseBean>> cache = new HashMap<>();
	List<PurgeListener> purgeListeners = new ArrayList<>();
	
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
			WeakValueMap<String, X2BaseBean> classCache = cache.get(beanType);
			if(classCache==null)
				return null;
			
			return classCache.get(oid);
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
			WeakValueMap<String, X2BaseBean> classCache = cache.get(bean.getClass());
			if(classCache==null) {
				classCache = new WeakValueMap<String, X2BaseBean>();
				for(PurgeListener purgeListener : purgeListeners) {
					classCache.addPurgeListener(purgeListener);
				}
				cache.put(bean.getClass(), classCache);
			}
			
			classCache.put(bean.getOid(), bean);
		}
	}
	
	/**
	 * Clear all data from this cache.
	 */
	public void clear() {
		synchronized(this) {
			for(WeakValueMap<String, X2BaseBean> m : cache.values()) {
				m.clear();
			}
			cache.clear();
		}
	}

	/**
	 * Clear all data related to a specific bean type from this cache.
	 */
	public void clear(Class<?> beanType) {
		synchronized(this) {
			WeakValueMap<String, X2BaseBean> classCache = cache.get(beanType);
			if(classCache==null)
				return;
			classCache.clear();
		}
	}

	public void addPurgeListener(PurgeListener purgeListener) {
		synchronized(this) {
			Objects.requireNonNull(purgeListener);
			purgeListeners.add(purgeListener);
			for(WeakValueMap<String, X2BaseBean> m : cache.values()) {
				m.addPurgeListener(purgeListener);
			}
		}
		
	}

	public void removePurgeListener(PurgeListener purgeListener) {
		synchronized(this) {
			if(purgeListeners.remove(purgeListener)) {
				for(WeakValueMap<String, X2BaseBean> m : cache.values()) {
					m.removePurgeListener(purgeListener);
				}
			}
		}
	}
}
