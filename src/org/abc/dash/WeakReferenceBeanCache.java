package org.abc.dash;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.pump.util.WeakValueMap;

/**
 * This simple cache keeps a WeakReference to beans.
 */
public class WeakReferenceBeanCache {

	/**
	 * This is the property attached to PropertyChangeEvents when the size of
	 * this cache changes.
	 */
	public static final String PROPERTY_SIZE = WeakReferenceBeanCache.class
			.getSimpleName() + "#size";

	protected Map<Class<?>, WeakValueMap<String, X2BaseBean>> cache = new HashMap<>();
	protected int cacheSize = 0;

	private PropertyChangeListener memberCacheSizeListener = new PropertyChangeListener() {

		@Override
		public void propertyChange(PropertyChangeEvent evt) {
			if (WeakValueMap.PROPERTY_SIZE.equals(evt.getPropertyName())) {
				int oldSize = (Integer) evt.getOldValue();
				int newSize = (Integer) evt.getNewValue();
				int oldCacheSize, newCacheSize;
				synchronized (WeakReferenceBeanCache.this) {
					oldCacheSize = cacheSize;
					cacheSize += newSize - oldSize;
					newCacheSize = cacheSize;
				}
				firePropertyChangeEvent(PROPERTY_SIZE, oldCacheSize,
						newCacheSize);
			}
		}
	};

	protected List<PropertyChangeListener> propertyListeners = new ArrayList<>();

	/**
	 * Add a PropertyChangeListener to this cache. This is notified when the
	 * {@link #PROPERTY_SIZE} property changes.
	 */
	public void addPropertyListener(PropertyChangeListener l) {
		Objects.requireNonNull(l);
		synchronized (this) {
			propertyListeners.add(l);
		}
	}

	protected void firePropertyChangeEvent(String propertyName,
			Object oldValue, Object newValue) {
		PropertyChangeListener[] array;
		synchronized (this) {
			array = propertyListeners
					.toArray(new PropertyChangeListener[propertyListeners
							.size()]);
		}
		for (PropertyChangeListener l : array) {
			l.propertyChange(new PropertyChangeEvent(this, propertyName,
					oldValue, newValue));
		}
	}

	public void removePropertyListener(PropertyChangeListener l) {
		synchronized (this) {
			propertyListeners.remove(l);
		}
	}

	/**
	 * Return a bean based on its class and oid, or return null if it is not
	 * in our cache.
	 * 
	 * @param beanType
	 *            the optional bean type. This is highly recommended, but it
	 *            will be automatically resolved based on the oid if it is null.
	 * @param oid
	 *            the bean oid to look up. If this is null then this method
	 *            immediately returns null.
	 * @return the bean that matches the oid argument, or null.
	 */
	public X2BaseBean getBeanByOid(Class<?> beanType, String oid) {
		// normally I'd throw an IllegalArgumentException if oid is null, but
		// X2Broker's accepted
		// convention is to allow null oids in X2Broker.getBeanByOid(), so for
		// parity's sake we'll
		// do the same here:
		if (oid == null)
			return null;

		if (beanType == null)
			beanType = Dash.getBeanTypeFromOid(oid);

		synchronized (this) {
			WeakValueMap<String, X2BaseBean> classCache = cache.get(beanType);
			if (classCache == null)
				return null;

			return classCache.get(oid);
		}
	}

	/**
	 * Store a WeakReference to the given bean in this cache.
	 * 
	 * @param bean
	 *            the bean to cache. If this is null then this method
	 *            immediately returns.
	 */
	public void storeBean(X2BaseBean bean) {
		if (bean == null)
			return;

		synchronized (this) {
			WeakValueMap<String, X2BaseBean> classCache = cache.get(bean
					.getClass());
			if (classCache == null) {
				classCache = new WeakValueMap<String, X2BaseBean>();
				classCache.addPropertyListener(memberCacheSizeListener);
				cache.put(bean.getClass(), classCache);
			}

			classCache.put(bean.getOid(), bean);
		}
	}

	/**
	 * Clear all data from this cache.
	 */
	public void clear() {
		synchronized (this) {
			for (WeakValueMap<String, X2BaseBean> m : cache.values()) {
				m.clear();
			}
			cache.clear();
		}
	}

	/**
	 * Clear all data related to a specific bean type from this cache.
	 * 
	 * @return the number of elements removed
	 */
	public int clear(Class<?> beanType) {
		if(beanType==null)
			return 0;
		
		synchronized (this) {
			WeakValueMap<String, X2BaseBean> classCache = cache.remove(beanType);
			if (classCache == null)
				return 0;
			int size = classCache.size();
			classCache.clear();
			return size;
		}
	}
}
