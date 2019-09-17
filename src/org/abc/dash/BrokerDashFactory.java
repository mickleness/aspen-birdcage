package org.abc.dash;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;

import org.abc.dash.BrokerDashSharedResource.CacheKey;
import org.apache.ojb.broker.query.Query;

import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.InsertQuery;
import com.follett.fsc.core.framework.persistence.UpdateQuery;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.pump.util.Cache;
import com.pump.util.Cache.CachePool;

/**
 * This converts X2Brokers into BrokerDashes.
 * <p>
 * All BrokerDashes created from the same BrokerDashFactory share the same
 * BrokerDashSharedResource / CachePool.
 */
public class BrokerDashFactory {

	static class DashInvocationHandler implements InvocationHandler {
		static Method method_getIteratorByQuery;
		static Method method_getBeanByQuery;
		static Method method_getSharedResource;
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
				method_getSharedResource = BrokerDash.class
						.getMethod("getSharedResource");
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
		BrokerDashSharedResource sharedResource;

		DashInvocationHandler(X2Broker broker,
				BrokerDashSharedResource sharedResource) {
			Objects.requireNonNull(broker);
			Objects.requireNonNull(sharedResource);
			this.broker = broker;
			this.sharedResource = sharedResource;
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

			if (method_getSharedResource.equals(method)) {
				return sharedResource;
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
				QueryIterator returnValue = sharedResource.createDashIterator(broker, beanQuery);
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
				sharedResource.clearAll();
			} else if ((method_deleteBean.equals(method) || method.getName()
					.startsWith("saveBean")) && args[0] instanceof X2BaseBean) {
				X2BaseBean bean = (X2BaseBean) args[0];
				Class t = bean.getClass();
				Cache<CacheKey, List<String>> cache = sharedResource.getCache(t, false);
				if (cache != null)
					cache.clear();
			} else if (method_deleteBeanByOid.equals(method)) {
				Class beanType = (Class) args[0];
				Cache<CacheKey, List<String>> cache = sharedResource.getCache(beanType, false);
				if (cache != null)
					cache.clear();
			} else if (method_deleteByQuery.equals(method)
					|| method_executeUpdateQuery.equals(method)
					|| method_executeInsertQuery.equals(method)) {
				Query query = (Query) args[0];
				Cache<CacheKey, List<String>> cache = sharedResource.getCache(query
						.getBaseClass(), false);
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
	}
	
	BrokerDashSharedResource sharedResource;

	public BrokerDashFactory(int cacheSizeLimit, long cacheDurationLimit) {
		CachePool cachePool = new CachePool(cacheSizeLimit, cacheDurationLimit, -1);
		sharedResource = new BrokerDashSharedResource(cachePool);
	}

	public BrokerDashFactory(BrokerDashSharedResource sharedResource) {
		Objects.requireNonNull(sharedResource);
		this.sharedResource = sharedResource;
	}
	
	public BrokerDashSharedResource getSharedResource() {
		return sharedResource;
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
			BrokerDashSharedResource sharedResource = bd.getSharedResource();
			if (sharedResource == this.sharedResource)
				return bd;
		}
		return createBrokerDash(broker);
	}

	@SuppressWarnings("rawtypes")
	private BrokerDash createBrokerDash(X2Broker broker) {
		InvocationHandler handler = new DashInvocationHandler(broker, sharedResource);

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
