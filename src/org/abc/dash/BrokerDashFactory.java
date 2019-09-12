package org.abc.dash;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
	
	static class Key {
		Operator operator;
		OrderByComparator orderBy;
		
		Key(Operator operator,OrderByComparator orderBy) {
			Objects.requireNonNull(orderBy);
			Objects.requireNonNull(operator);
			this.operator = operator;
			this.orderBy = orderBy;
		}

		@Override
		public int hashCode() {
			return operator.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof Key))
				return false;
			Key other = (Key) obj;
			if(!operator.equals(other.operator))
				return false;
			if(!orderBy.equals(other.orderBy))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Key[ operator=\""+operator+"\", orderBy=\""+orderBy+"\"]";
		}
	}

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
	 * Return all the beans in a list of bean oids, or null if those beans were not readily available in the global cache.
	 */
	public static List<X2BaseBean> getBeansFromGlobalCache(PersistenceKey persistenceKey,Class beanType,List<String> beanOids) {
		List<X2BaseBean> beans = new ArrayList<>(beanOids.size());
		for(String beanOid : beanOids) {
			X2BaseBean bean = getBeanFromGlobalCache(persistenceKey, beanType, beanOid);
			if(bean==null)
				return null;
			beans.add(bean);
		}
		return beans;
	}
	
	protected static class BeanIteratorFromList extends QueryIterator {
		Iterator<X2BaseBean> beanIter;
		QueryIterator queryIter;
		PersistenceKey persistenceKey;

		/**
		 * Create an iterator that will walk through a collection of beans.
		 */
		public BeanIteratorFromList(PersistenceKey persistenceKey,Collection<X2BaseBean> beans) {
			this(persistenceKey, beans, null);
		}

		/**
		 * Create an iterator that will walk through a collection of beans and then a QueryIterator.
		 */
		public BeanIteratorFromList(PersistenceKey persistenceKey, Collection<X2BaseBean> beans, QueryIterator queryIterator) {
			Objects.requireNonNull(persistenceKey);
			beanIter = beans==null ? null : beans.iterator();
			queryIter = queryIterator;
			this.persistenceKey = persistenceKey;
		}

		@Override
		protected void finalize() {
			close();
		}

		@Override
		public void close() {
			if(queryIter!=null)
				queryIter.close();
			queryIter = null;
			beanIter = null;
		}

		@Override
		protected Iterator getIterator(PersistenceBroker arg0, Query arg1) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object next() {
			Object returnValue = null;
			if(beanIter!=null) {
				if(beanIter.hasNext())
					returnValue = beanIter.next();
				if(!beanIter.hasNext())
					beanIter = null;
			}
			if(returnValue==null && queryIter!=null) {
				if(queryIter.hasNext())
					returnValue = queryIter.next();
				if(!queryIter.hasNext())
					queryIter = null;
			}
			return returnValue;
		}

		@Override
		public boolean hasNext() {
			if(beanIter!=null && beanIter.hasNext())
				return true;
			if(queryIter!=null && queryIter.hasNext())
				return true;
			return false;
		}

		@Override
		public PersistenceKey getPersistenceKey() {
			return persistenceKey;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
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
		Map<Class, Cache<Key, List<String>>> cacheByBeanType;

		DashInvocationHandler(X2Broker broker,
				Map<Class, Cache<Key, List<String>>> cacheByBeanType,
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
				Cache<Key, List<String>> cache = cacheByBeanType.get(t);
				if (cache != null)
					cache.clear();
			} else if (method_deleteBeanByOid.equals(method)) {
				Class beanType = (Class) args[0];
				Cache<Key, List<String>> cache = cacheByBeanType
						.get(beanType);
				if (cache != null)
					cache.clear();
			} else if (method_deleteByQuery.equals(method)
					|| method_executeUpdateQuery.equals(method)
					|| method_executeInsertQuery.equals(method)) {
				Query query = (Query) args[0];
				Cache<Key, List<String>> cache = cacheByBeanType.get(query
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
		protected QueryIterator createdCachedIterator(BeanQuery beanQuery) {
			Operator operator = CriteriaToOperatorConverter
					.createOperator(beanQuery.getCriteria());
			OrderByComparator orderBy = new OrderByComparator(false,
					beanQuery.getOrderBy());
			final Key cacheKey = new Key(operator, orderBy);
			Cache<Key, List<String>> cache = cacheByBeanType.get(beanQuery
					.getBaseClass());
			if (cache == null) {
				cache = new Cache(cachePool);
				cacheByBeanType.put(beanQuery.getBaseClass(), cache);
			}

			List<String> beanOids = cache.get(cacheKey);
			if (beanOids != null) {
				//this is our ideal case: we know the complete query results
				return new BeanIteratorFromOidList(broker,
						beanQuery.getBaseClass(), beanOids);
			}
			
			Operator canonicalOperator = operator.getCanonicalOperator();
			
			//if a criteria said: "A==1 or A==2", then this splits them into two
			//unique criteria "A==1" and "A==2"
			
			Collection<Operator> splitOperators = canonicalOperator.split();
			splitOperators.remove(canonicalOperator);
			
			Iterator<Operator> splitIter = splitOperators.iterator();
			Collection<X2BaseBean> knownBeans = new TreeSet<>(orderBy);
			boolean removedOperators = false;
			while(splitIter.hasNext()) {
				Operator splitOperator = splitIter.next();
				Key splitKey = new Key(splitOperator, orderBy);
				List<String> splitOids = cache.get(splitKey);
				List<X2BaseBean> splitBeans = getBeansFromGlobalCache(broker.getPersistenceKey(), 
						beanQuery.getBaseClass(), 
						splitOids);
				if(splitBeans!=null) {
					removedOperators = true;
					knownBeans.addAll(splitBeans);
					splitIter.remove();
				}
			}
			
			if(removedOperators) {
				if(splitOperators.isEmpty()) {
					// This is near-ideal: we broke the criteria down into small 
					// pieces and looked up each piece. We only had to pay the cost
					// of sorting. (That cost may include queries to look up order-by attributes, though)
					return new BeanIteratorFromList(broker.getPersistenceKey(), knownBeans);
				}
				
				// we resolved *some* operators, but not all of them.
				
				Operator trimmedOperator = Operator.join(splitOperators.toArray(new Operator[splitOperators.size()]));
				Criteria trimmedCriteria = CriteriaToOperatorConverter.createCriteria(trimmedOperator);
				beanQuery = new BeanQuery(beanQuery.getBaseClass(), trimmedCriteria);
			} else {
				// we have a blank slate (no cached info came up)
			
				// so let's just dump incoming beans in a list. The order is going to be correct,
				// because the order is coming straight from the source:
				knownBeans = new LinkedList<>();
			}

			// grab the first 1000 beans from our query:
			QueryIterator iter = broker.getIteratorByQuery(beanQuery);
			int ctr = 0;
			while(iter.hasNext() && ctr<1000) {
				X2BaseBean bean = (X2BaseBean) iter.next();
				knownBeans.add(bean);
				ctr++;
			}
			
			if(iter.hasNext()) {
				// too many beans; let's give up on caching:
				return new BeanIteratorFromList(broker.getPersistenceKey(), knownBeans, iter);
			}
			
			// we exhausted the iterator, so we have ALL the beans.
			
			// now we have all our data, but before we return let's cache it for future lookups:
			
			List<String> knownBeanOids = new LinkedList<>();
			for(X2BaseBean bean : knownBeans) {
				knownBeanOids.add(bean.getOid());
			}
			
			cache.put(cacheKey, knownBeanOids);
			
			Map<Operator, List<String>> oidsByOperator = new HashMap<>();
			for(X2BaseBean bean : knownBeans) {
				for(Operator op : splitOperators) {
					if(op.evaluate(BrokerDash.CONTEXT, bean)) {
						List<String> oids = oidsByOperator.get(op);
						if(oids==null) {
							oids = new LinkedList<>();
							oidsByOperator.put(op, oids);
						}
						oids.add(bean.getOid());
					}
				}
			}
			
			for(Entry<Operator, List<String>> entry : oidsByOperator.entrySet()) {
				Key splitKey = new Key(entry.getKey(), orderBy);
				cache.put(splitKey, entry.getValue());
			}

			return new BeanIteratorFromList(broker.getPersistenceKey(), knownBeans);
		}
	}

	protected CachePool cachePool;

	@SuppressWarnings("rawtypes")
	Map<Class, Cache<Key, List<String>>> cacheByBeanType;

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
