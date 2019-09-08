package org.abc.dash;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.logging.Level;

import org.abc.util.OrderByComparator;
import org.apache.ojb.broker.Identity;
import org.apache.ojb.broker.PersistenceBroker;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.Query;

import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.InsertQuery;
import com.follett.fsc.core.framework.persistence.UpdateQuery;
import com.follett.fsc.core.framework.persistence.X2ObjectCache;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.pump.data.operator.Operator;
import com.pump.util.Cache;
import com.pump.util.Cache.CachePool;

/**
 * This converts X2Brokers into BrokerDashes.
 * <p>
 * All BrokerDashes created from the same BrokerDashFactory share the same set
 * of Caches. This may be useful when you're trying to write a multithreaded
 * tool: each thread needs its own broker, but each of those brokers can share
 * the same cache.
 */
public class BrokerDashFactory {

	/**
	 * Return a bean from the global cache, or return null if the bean does not
	 * exist in the cache.
	 * 
	 * @param persistenceKey
	 *            the key identifying the database/client.
	 * @param beanClass
	 *            the type of bean to return. This is highly recommended for
	 *            efficiency's sake, but if this is left null it will be derived
	 *            from the bean oid.
	 * @param beanOid
	 *            the oid of the bean to return.
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static X2BaseBean getBeanFromGlobalCache(
			PersistenceKey persistenceKey, Class beanClass, String beanOid) {
		Objects.requireNonNull(persistenceKey);
		Objects.requireNonNull(beanOid);

		X2ObjectCache x2cache = AppGlobals.getCache(persistenceKey);

		if (beanClass == null) {
			String prefix = beanOid.substring(0, Math.min(3, beanOid.length()));
			BeanTablePath btp = BeanTablePath.getTableByName(prefix
					.toUpperCase());
			if (btp == null)
				throw new IllegalArgumentException(
						"Could not determine bean class for \"" + beanOid
								+ "\".");
			beanClass = btp.getBeanType();
		}

		Identity identity = new Identity(beanClass, beanClass,
				new Object[] { beanOid });
		X2BaseBean bean = (X2BaseBean) x2cache.lookup(identity);
		return bean;
	}

	/**
	 * This filters a QueryIterator and records the oids of the beans it
	 * returns.
	 * <p>
	 * When this iterator is closed: that list of oids will be stored in a Cache
	 * using a predetermined key.
	 * <p>
	 * This list only stores a fixed number of oids. If the iterator exceeds
	 * this limit: then this wrapper gives up and nothing is cached. For
	 * example: if it iterates over, say, 500 beans that's probably OK. But if
	 * this iterates over 5 million beans this just quietly gives up monitoring
	 * after a fixed amount.
	 */
	@SuppressWarnings("rawtypes")
	protected static class BeanIteratorWrapper extends QueryIterator {

		// Design-wise: this class is kind of hackish because it relies on
		// QueryIterator's empty constructor. (But, in our defense:
		// QueryIterator really ought to be an interface... which would make
		// relying on a quirky constructor a non-issue...)

		QueryIterator iter;
		int maxListSize;
		boolean active = true;
		Cache<Object, List<String>> cache;
		Object cacheKey;
		List<String> oids = new LinkedList<>();

		/**
		 * @param cache
		 *            the cache to store the list of oids in when close() is
		 *            called
		 * @param cacheKey
		 *            the key to use to store the list of oids when close() is
		 *            called
		 * @param iter
		 *            the underlying delegate iterator that's supplying all our
		 *            info
		 * @param maxListSize
		 *            the maximum number of oids this wrapper will record before
		 *            giving up
		 */
		public BeanIteratorWrapper(Cache<Object, List<String>> cache,
				Object cacheKey, QueryIterator iter, int maxListSize) {
			Objects.requireNonNull(cache);
			Objects.requireNonNull(cacheKey);
			Objects.requireNonNull(iter);
			this.iter = iter;
			this.cache = cache;
			this.cacheKey = cacheKey;
			this.maxListSize = maxListSize;
		}

		@Override
		public Object next() {
			X2BaseBean bean = (X2BaseBean) iter.next();
			if (active) {
				oids.add(bean.getOid());
				if (oids.size() > maxListSize) {
					active = false;
				}
			}
			return bean;
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public PersistenceKey getPersistenceKey() {
			return iter.getPersistenceKey();
		}

		@Override
		public void close() {
			// if there are "a few" more elements: go ahead and record those
			// too.

			for (int a = 0; active && a < 100; a++) {
				if (hasNext()) {
					next();
				} else {
					break;
				}
			}
			if (iter.hasNext()) {
				// we should give up. We don't know how many more elements
				// are waiting for us.
				active = false;
			}

			if (active) {
				cache.put(cacheKey, oids);
			}
			iter.close();
		}

		@Override
		protected Iterator getIterator(PersistenceBroker arg0, Query arg1) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void remove() {
			iter.remove();
		}
	}

	/**
	 * This iterates over a series of X2BaseBeans based on an ordered list of
	 * bean oids.
	 * <p>
	 * This pulls beans out of the X2ObjectCache when possible. If some beans
	 * are not in that cache: then this will execute one query to identify the
	 * remaining beans based on their oids.
	 */
	@SuppressWarnings("rawtypes")
	static class BeanIteratorFromOidList extends QueryIterator {
		X2Broker broker;
		List<String> beanOids;
		Class beanType;

		Iterator<String> beanOidsIter;

		/**
		 * This is an ordered set of the remaining beans. This field will only
		 * be defined if the X2ObjectCache failed at some point to provide a
		 * required bean. Once this field is defined: we use this data insetad
		 * of beanOidsIter
		 */
		TreeSet<X2BaseBean> beans;

		public BeanIteratorFromOidList(X2Broker broker, Class beanType,
				List<String> beanOids) {
			Objects.requireNonNull(broker);
			Objects.requireNonNull(beanOids);
			Objects.requireNonNull(beanType);
			this.broker = broker;
			this.beanOids = beanOids;
			this.beanType = beanType;
			beanOidsIter = beanOids.iterator();
		}

		@Override
		protected void finalize() {
			close();
		}

		@Override
		public boolean hasNext() {
			if (beans != null)
				return !beans.isEmpty();

			return beanOidsIter.hasNext();
		}

		@Override
		public Object next() {
			if (beans != null) {
				// in this case: we already had to do the hard work (see below),
				// and now we know exactly what to return.
				return beans.pollFirst();
			}

			String beanOid = beanOidsIter.next();
			X2BaseBean bean = BrokerDashFactory.getBeanFromGlobalCache(
					broker.getPersistenceKey(), beanType, beanOid);
			if (bean != null) {
				// this is our ideal case: we asked for a bean oid and the cache
				// provided the bean.
				return bean;
			}

			// This is unfortunate: the global cache doesn't have what we need.
			// So now we're going to actually fetch all the remaining beans we
			// need and from now on we'll iterate over that.

			List<String> beanOidsToQuery = new LinkedList<>();
			final Map<String, Integer> beanOidToOrder = new HashMap<>();
			Comparator<X2BaseBean> comparator = new Comparator<X2BaseBean>() {

				@Override
				public int compare(X2BaseBean o1, X2BaseBean o2) {
					int k1 = beanOidToOrder.get(o1.getOid());
					int k2 = beanOidToOrder.get(o2.getOid());
					return Integer.compare(k1, k2);
				}

			};

			beans = new TreeSet<>(comparator);

			int ctr = 0;
			beanOidToOrder.put(beanOid, ctr++);
			beanOidsToQuery.add(beanOid);

			while (beanOidsIter.hasNext()) {
				beanOid = beanOidsIter.next();
				beanOidToOrder.put(beanOid, ctr++);

				bean = BrokerDashFactory.getBeanFromGlobalCache(
						broker.getPersistenceKey(), beanType, beanOid);
				if (bean != null) {
					// go ahead and keep strong references to all the remaining
					// beans. If the X2ObjectCache is purged we don't want to
					// issue any more queries to resolve missing beans.
					beans.add(bean);
				} else {
					beanOidsToQuery.add(beanOid);
				}
			}

			Criteria criteria = new Criteria();
			criteria.addIn(X2BaseBean.COL_OID, beanOidsToQuery);
			BeanQuery beanQuery = new BeanQuery(beanType, criteria);
			try (QueryIterator iter = broker.getIteratorByQuery(beanQuery)) {
				while (iter.hasNext()) {
					bean = (X2BaseBean) iter.next();
					beans.add(bean);
				}
			}

			return beans.pollFirst();
		}

		@Override
		public PersistenceKey getPersistenceKey() {
			return broker.getPersistenceKey();
		}

		@Override
		public void close() {
			// do nothing
		}

		@Override
		protected Iterator getIterator(PersistenceBroker arg0, Query arg1) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	static class DashInvocationHandler implements InvocationHandler {
		static Method method_getIteratorByQuery;
		static Method method_getBeanByQuery;
		static Method method_getCachePool;
		static Method method_clearCache;
		static Method method_rollbackTransaction1;
		static Method method_rollbackTransaction2;
		static Method method_deleteBean;
		static Method method_deleteBeanByOid;
		static Method method_deleteByQuery;
		static Method method_executeUpdateQuery;
		static Method method_executeInsertQuery;
		static Method method_getCollectionByQuery;
		static Method method_getGroupedCollectionByQuery1;
		static Method method_getGroupedCollectionByQuery2;
		static Method method_getMapByQuery;
		static Method method_getNestedMapByQuery1;
		static Method method_getNestedMapByQuery2;
		static boolean initialized = false;

		static {
			try {
				method_getCachePool = BrokerDash.class
						.getMethod("getCachePool");
				method_getIteratorByQuery = X2Broker.class.getMethod(
						"getIteratorByQuery", Query.class);
				method_getBeanByQuery = X2Broker.class.getMethod(
						"getBeanByQuery", Query.class);
				method_clearCache = X2Broker.class.getMethod("clearCache");
				method_rollbackTransaction1 = X2Broker.class
						.getMethod("rollbackTransaction");
				method_rollbackTransaction2 = X2Broker.class.getMethod(
						"rollbackTransaction", Boolean.TYPE);
				method_deleteBean = X2Broker.class.getMethod("deleteBean",
						X2BaseBean.class);
				method_deleteBeanByOid = X2Broker.class.getMethod(
						"deleteBeanByOid", Class.class, String.class);
				method_deleteByQuery = X2Broker.class.getMethod(
						"deleteByQuery", Query.class);
				method_executeUpdateQuery = X2Broker.class.getMethod(
						"executeUpdateQuery", UpdateQuery.class);
				method_executeInsertQuery = X2Broker.class.getMethod(
						"executeInsertQuery", InsertQuery.class);
				method_getCollectionByQuery = X2Broker.class.getMethod(
						"getCollectionByQuery", Query.class);
				method_getGroupedCollectionByQuery1 = X2Broker.class.getMethod(
						"getGroupedCollectionByQuery", Query.class,
						String.class, Integer.TYPE);
				method_getGroupedCollectionByQuery2 = X2Broker.class.getMethod(
						"getGroupedCollectionByQuery", Query.class,
						String[].class, new int[0].getClass());
				method_getMapByQuery = X2Broker.class.getMethod(
						"getMapByQuery", Query.class, String.class,
						Integer.TYPE);
				method_getNestedMapByQuery1 = X2Broker.class.getMethod(
						"getNestedMapByQuery", Query.class, String.class,
						String.class, Integer.TYPE, Integer.TYPE);
				method_getNestedMapByQuery2 = X2Broker.class.getMethod(
						"getNestedMapByQuery", Query.class, String[].class,
						new int[0].getClass());
				initialized = true;
			} catch (Exception e) {
				AppGlobals
						.getLog()
						.log(Level.SEVERE,
								"An error occurred initializing the Dash cache architecture, so it is being deactivated.",
								e);
			}
		}

		X2Broker broker;
		boolean active = initialized;
		CachePool cachePool;
		Map<Class, Cache<Object, List<String>>> cacheByBeanType;

		DashInvocationHandler(X2Broker broker,
				Map<Class, Cache<Object, List<String>>> cacheByBeanType,
				CachePool cachePool) {
			Objects.requireNonNull(broker);
			Objects.requireNonNull(cacheByBeanType);
			Objects.requireNonNull(cachePool);
			this.cacheByBeanType = cacheByBeanType;
			this.broker = broker;
			this.cachePool = cachePool;
		}

		protected void handleException(Exception e) {
			AppGlobals
					.getLog()
					.log(Level.SEVERE,
							"An error occurred using the Dash cache architecture, so it is being deactivated.",
							e);
			active = false;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {

			// handle methods unique to the BrokerDash interface:

			if (method_getCachePool.equals(method)) {
				return cachePool;
			}

			// if possible: intercept methods using our caching model/layer

			if (active) {
				try {
					return invokeCached(proxy, method, args);
				} catch (Exception e) {
					handleException(e);
				}
			}

			// ... if that fails: invoke the method without our caching
			// model/layer:

			return method.invoke(broker, args);
		}

		@SuppressWarnings({ "rawtypes" })
		protected Object invokeCached(Object proxy, Method method, Object[] args)
				throws Throwable {
			if (method_getIteratorByQuery.equals(method)
					&& args[0] instanceof BeanQuery) {
				BeanQuery beanQuery = (BeanQuery) args[0];
				QueryIterator returnValue = createdCachedIterator(beanQuery);
				return returnValue;
			} else if (method_getBeanByQuery.equals(method)
					&& args[0] instanceof BeanQuery) {
				// pass through getIteratorByQuery to benefit from our caching
				try (QueryIterator queryIter = (QueryIterator) invoke(proxy,
						method_getIteratorByQuery, args)) {
					if (queryIter.hasNext())
						return (X2BaseBean) queryIter.next();
					return null;
				}
			} else if (method_getCollectionByQuery.equals(method)
					&& args[0] instanceof BeanQuery) {
				// pass through getIteratorByQuery to benefit from our caching
				try (QueryIterator queryIter = (QueryIterator) invoke(proxy,
						method_getIteratorByQuery, args)) {
					Collection<X2BaseBean> result = new ArrayList<>();
					while (queryIter.hasNext()) {
						X2BaseBean bean = (X2BaseBean) queryIter.next();
						result.add(bean);
					}
					return result;
				}
			} else if (method_getGroupedCollectionByQuery1.equals(method)
					&& args[0] instanceof BeanQuery) {
				Object[] newArgs = new Object[] { args[0],
						new String[] { (String) args[1] },
						new int[] { ((Integer) args[2]).intValue() } };
				return invoke(proxy, method_getGroupedCollectionByQuery2,
						newArgs);
			} else if (method_getGroupedCollectionByQuery2.equals(method)
					&& args[0] instanceof BeanQuery) {
				BeanQuery query = (BeanQuery) args[0];
				String[] columns = (String[]) args[1];
				int[] mapSizes = (int[]) args[2];
				return createNestedMap(proxy, query, columns, mapSizes, true);
			} else if (method_getMapByQuery.equals(method)
					&& args[0] instanceof BeanQuery) {
				Object[] newArgs = new Object[] { args[0],
						new String[] { (String) args[1] },
						new int[] { ((Integer) args[2]).intValue() } };
				return invoke(proxy, method_getNestedMapByQuery2, newArgs);
			} else if (method_getNestedMapByQuery1.equals(method)
					&& args[0] instanceof BeanQuery) {
				Object[] newArgs = new Object[] {
						args[0],
						new String[] { (String) args[1], (String) args[2] },
						new int[] { ((Integer) args[3]).intValue(),
								((Integer) args[4]).intValue() } };
				return invoke(proxy, method_getNestedMapByQuery2, newArgs);
			} else if (method_getNestedMapByQuery2.equals(method)
					&& args[0] instanceof BeanQuery) {
				BeanQuery query = (BeanQuery) args[0];
				String[] columns = (String[]) args[1];
				int[] mapSizes = (int[]) args[2];
				return createNestedMap(proxy, query, columns, mapSizes, false);
			} else if (method_clearCache.equals(method)
					|| method_rollbackTransaction1.equals(method)
					|| method_rollbackTransaction2.equals(method)) {
				cachePool.clear();
			} else if ((method_deleteBean.equals(method) || method.getName()
					.startsWith("saveBean")) && args[0] instanceof X2BaseBean) {
				X2BaseBean bean = (X2BaseBean) args[0];
				Class t = bean.getClass();
				Cache<Object, List<String>> cache = cacheByBeanType.get(t);
				if (cache != null)
					cache.clear();
			} else if (method_deleteBeanByOid.equals(method)) {
				Class beanType = (Class) args[0];
				Cache<Object, List<String>> cache = cacheByBeanType
						.get(beanType);
				if (cache != null)
					cache.clear();
			} else if (method_deleteByQuery.equals(method)
					|| method_executeUpdateQuery.equals(method)
					|| method_executeInsertQuery.equals(method)) {
				Query query = (Query) args[0];
				Cache<Object, List<String>> cache = cacheByBeanType.get(query
						.getBaseClass());
				if (cache != null)
					cache.clear();
			}

			return method.invoke(broker, args);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private Object createNestedMap(Object proxy, BeanQuery query,
				String[] columns, int[] mapSizes, boolean useLists)
				throws Throwable {
			// pass through getIteratorByQuery to benefit from our caching
			try (QueryIterator queryIter = (QueryIterator) invoke(proxy,
					method_getIteratorByQuery, new Object[] { query })) {

				Map map = new HashMap(mapSizes[0]);
				while (queryIter.hasNext()) {
					X2BaseBean bean = (X2BaseBean) queryIter.next();
					Map currentMap = map;
					for (int a = 0; a < columns.length; a++) {
						// TODO: getFieldValueByBeanPath might invoke its own
						// queries. We could instead use a ColumnQuery to
						// include this in the query if needed
						Object key = bean.getFieldValueByBeanPath(columns[a]);
						if (a == columns.length - 1) {
							if (useLists) {
								List list = (List) currentMap.get(key);
								if (list == null) {
									list = new LinkedList();
									currentMap.put(key, list);
								}
								list.add(bean);
							} else {
								currentMap.put(key, bean);
							}
						} else {
							Map innerMap = (Map) currentMap.get(key);
							if (innerMap == null) {
								innerMap = new HashMap(mapSizes[a + 1]);
								currentMap.put(key, innerMap);
							}
							currentMap = innerMap;
						}
					}
				}
				return map;
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		protected QueryIterator createdCachedIterator(final BeanQuery beanQuery) {
			final Operator operator = CriteriaToOperatorConverter
					.createOperator(beanQuery.getCriteria());
			OrderByComparator orderBy = new OrderByComparator(false,
					beanQuery.getOrderBy());
			final Object cacheKey = Arrays.asList(operator, orderBy);
			Cache<Object, List<String>> cache = cacheByBeanType.get(beanQuery
					.getBaseClass());
			if (cache == null) {
				cache = new Cache(cachePool);
				cacheByBeanType.put(beanQuery.getBaseClass(), cache);
			}

			List<String> beanOids = cache.get(cacheKey);
			if (beanOids != null)
				return new BeanIteratorFromOidList(broker,
						beanQuery.getBaseClass(), beanOids);

			QueryIterator iter = broker.getIteratorByQuery(beanQuery);
			return new BeanIteratorWrapper(cache, cacheKey, iter, 1000);
		}
	}

	protected CachePool cachePool;

	@SuppressWarnings("rawtypes")
	Map<Class, Cache<Object, List<String>>> cacheByBeanType;

	public BrokerDashFactory(int cacheSizeLimit, long cacheDurationLimit) {
		cachePool = new CachePool(cacheSizeLimit, cacheDurationLimit, -1);
		cacheByBeanType = new HashMap<>();
	}

	/**
	 * Create a BrokerDash that uses this factory's CachePool.
	 * <p>
	 * It is safe to call this method redundantly. If the argument already uses
	 * this factory's ThreadPool then the argument is returned as-is.
	 */
	public BrokerDash convertToBrokerDash(X2Broker broker) {
		if (broker instanceof BrokerDash) {
			BrokerDash bd = (BrokerDash) broker;
			CachePool cachePool = bd.getCachePool();
			if (cachePool == this.cachePool)
				return bd;
		}
		return createBrokerDash(broker);
	}

	private BrokerDash createBrokerDash(X2Broker broker) {
		InvocationHandler handler = new DashInvocationHandler(broker,
				cacheByBeanType, cachePool);

		// include all the old interfaces, in case something else like BrokerCub
		// was already attached
		Class[] oldInterfaces = broker.getClass().getInterfaces();
		Class[] newInterfaces = new Class[oldInterfaces.length + 1];
		System.arraycopy(oldInterfaces, 0, newInterfaces, 0,
				oldInterfaces.length);
		newInterfaces[newInterfaces.length - 1] = BrokerDash.class;

		return (BrokerDash) Proxy.newProxyInstance(
				BrokerDash.class.getClassLoader(), newInterfaces, handler);
	}
}
