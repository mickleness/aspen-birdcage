package org.abc.dash;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.ojb.broker.query.Query;
import org.apache.ojb.broker.query.QueryByCriteria;

import com.follett.fsc.core.framework.persistence.InsertQuery;
import com.follett.fsc.core.framework.persistence.UpdateQuery;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.X2Broker;
import com.x2dev.utils.LoggerUtils;
import com.x2dev.utils.ThreadUtils;

/**
 * This implements the BrokerDash (and all the X2Broker methods).
 * <p>
 * Certain calls are intercepted/optimized, and all other calls are
 * delegated to an existing X2Broker.
 * <p>
 * This is implemented dynamically (using an InvocationHandler) because
 * we assume methods will continue to be added to the X2Broker interface
 * over time. If we created a class that actually implemented X2Broker:
 * then our class would be missing methods as the X2Broker is enhanced over
 * time. This approach, by contrast, should support new methods because
 * it will just pass anything it doesn't recognize to its delegate.
 * <p>
 * Like all brokers: this object is not thread-safe; it should only
 * be used on one thread at a time.
 */
class DashInvocationHandler implements InvocationHandler {
	static RuntimeException constructionException;
	static Method method_getIteratorByQuery;
	static Method method_getBeanByQuery;
	static Method method_getDash;
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
	static Method method_getBeanByOid;
	static boolean initialized = false;

	static {
		try {
			method_getDash = BrokerDash.class.getMethod("getDash");
			method_getIteratorByQuery = X2Broker.class.getMethod(
					"getIteratorByQuery", Query.class);
			method_getBeanByQuery = X2Broker.class.getMethod("getBeanByQuery",
					Query.class);
			method_clearCache = X2Broker.class.getMethod("clearCache");
			method_rollbackTransaction1 = X2Broker.class
					.getMethod("rollbackTransaction");
			method_rollbackTransaction2 = X2Broker.class.getMethod(
					"rollbackTransaction", Boolean.TYPE);
			method_deleteBean = X2Broker.class.getMethod("deleteBean",
					X2BaseBean.class);
			method_deleteBeanByOid = X2Broker.class.getMethod(
					"deleteBeanByOid", Class.class, String.class);
			method_deleteByQuery = X2Broker.class.getMethod("deleteByQuery",
					Query.class);
			method_executeUpdateQuery = X2Broker.class.getMethod(
					"executeUpdateQuery", UpdateQuery.class);
			method_executeInsertQuery = X2Broker.class.getMethod(
					"executeInsertQuery", InsertQuery.class);
			method_getCollectionByQuery = X2Broker.class.getMethod(
					"getCollectionByQuery", Query.class);
			method_getGroupedCollectionByQuery1 = X2Broker.class.getMethod(
					"getGroupedCollectionByQuery", Query.class, String.class,
					Integer.TYPE);
			method_getGroupedCollectionByQuery2 = X2Broker.class.getMethod(
					"getGroupedCollectionByQuery", Query.class, String[].class,
					new int[0].getClass());
			method_getMapByQuery = X2Broker.class.getMethod("getMapByQuery",
					Query.class, String.class, Integer.TYPE);
			method_getNestedMapByQuery1 = X2Broker.class.getMethod(
					"getNestedMapByQuery", Query.class, String.class,
					String.class, Integer.TYPE, Integer.TYPE);
			method_getNestedMapByQuery2 = X2Broker.class.getMethod(
					"getNestedMapByQuery", Query.class, String[].class,
					new int[0].getClass());
			method_getBeanByOid = X2Broker.class.getMethod("getBeanByOid",
					Class.class, String.class);
			initialized = true;
		} catch (Exception e) {
			constructionException = new RuntimeException(
					"An error occurred initializing the Dash cache architecture, so it is being deactivated.",
					e);
		}
	}

	/**
	 * This indents (pads) LogRecord messages a few spaces.
	 */
	static class IndentionHandler extends Handler {

		int spaces = 2;

		@Override
		public void publish(LogRecord record) {
			StringBuilder sb = new StringBuilder();
			for (int a = 0; a < spaces; a++) {
				sb.append(" ");
			}
			String msg = record.getMessage();
			sb.append(msg);
			record.setMessage(sb.toString());
		}

		@Override
		public void flush() {
		}

		@Override
		public void close() throws SecurityException {
		}

		public void increase() {
			spaces += 2;
		}

	}

	X2Broker broker;
	boolean active = initialized;
	Dash dash;

	/**
	 * Create a new DashInvocationHandler.
	 * 
	 * @param broker the broker all requests are eventually passed to if we
	 * cannot optimize them.
	 * @param dash the Dash used to manage the cached layer/data.
	 */
	DashInvocationHandler(X2Broker broker, Dash dash) {
		if (constructionException != null)
			throw constructionException;
		Objects.requireNonNull(broker);
		Objects.requireNonNull(dash);
		this.broker = broker;
		this.dash = dash;
	}
	
	private IndentionHandler indentHandler;
	long lastPurge = -1;

	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		ThreadUtils.checkInterrupt();
		
		// The following purges (some) cached info every 30 seconds.
		// This is NOT essential for the caches to drop elements.
		// Every time either object (weakReferenceCache or cachePool)
		// is used: it will also look to see if it can purge expired
		// or unusable data.
		
		// ... but where this might come handy is this scenario:
		// If you've used weakReferenceCache and created a cache
		// relating to SisStudents, and then you proceed to not
		// call that particular cache (related to SisStudents) for
		// a long time: we may benefit from explicitly purging that
		// cache. If we do not: in a worst-case scenario that cache
		// may keep a large number of Strings and unusable WeakReferences
		// in memory.
		
		long currentTime = System.currentTimeMillis();
		long elapsed = currentTime - lastPurge;
		if(elapsed>30000) {
			dash.weakReferenceCache.purge();
			dash.cachePool.purge();
			lastPurge = currentTime;
		}

		Logger log = dash.getLog();
		boolean indentHandlerCreated;
		if(indentHandler==null) {
			indentHandler = indentLog(log);
			indentHandlerCreated = indentHandler!=null;
		} else {
			indentHandlerCreated = false;
		}
		try {
			logMethod(Level.FINER, method, args, null);
			
			// handle methods unique to the BrokerDash interface:
			if (method_getDash.equals(method)) {
				return dash;
			} else if (method.getName().equals("setDashActive")) {
				active = ((Boolean) args[0]).booleanValue();
				return Void.TYPE;
			} else if (method.getName().equals("isDashActive")) {
				return active;
			}
			
			// if possible: intercept methods using our caching model/layer
			if (active) {
				try {
					return invokeCached(proxy, method, args);
				} catch(CancellationException e) {
					throw e;
				} catch (Exception e) {
					Exception e2 = new Exception(
							"An error occurred using the Dash cache architecture, so it is being deactivated.",
							e);
					UncaughtExceptionHandler ueh = dash.getUncaughtExceptionHandler();
					if (ueh != null) {
						ueh.uncaughtException(Thread.currentThread(), e2);
					}

					if (log != null && log.isLoggable(Level.WARNING))
						log.warning(LoggerUtils.convertThrowableToString(e2));

					active = false;
				}
			}

			// ... if that fails: invoke the method without our caching
			// model/layer:

			long t = System.currentTimeMillis();
			Object returnValue = method.invoke(broker, args);
			t = System.currentTimeMillis() - t;
			
			if (t > 10 && log != null) {
				logMethod(Level.FINEST, method, args, "(ended)");
			}

			return returnValue;
		} finally {
			if(indentHandlerCreated) {
				log.removeHandler(indentHandler);
				indentHandler = null;
			}
		}
	}

	/**
	 * Install an IndentionHandler as the zeroeth handler on our Log
	 * (so it indents messages before any other Handlers are notified).
	 * 
	 * @return the IndentionHandler, or null if it couldn't be created
	 */
	private IndentionHandler indentLog(Logger log) {
		if (log == null)
			return null;
		// insert our handler as the zero-eth handler so it grabs messages
		// before anyone else
		IndentionHandler indentHandler = new IndentionHandler();
		Handler[] oldHandlers = log.getHandlers();
		for (int a = 0; a < oldHandlers.length; a++) {
			log.removeHandler(oldHandlers[a]);
		}
		log.addHandler(indentHandler);
		for (int a = 0; a < oldHandlers.length; a++) {
			log.addHandler(oldHandlers[a]);
		}
		return indentHandler;
	}

	/**
	 * Invoke a method using the Dash caching model.
	 */
	@SuppressWarnings({ "rawtypes" })
	protected Object invokeCached(Object proxy, Method method, Object[] args) throws Throwable {
		// our first rule here should be: do no harm.
		// So if someone passes null in as an argument: we should just skip
		// over our caching implementation. If the parent broker wants to throw
		// a NPE: that's great. But adding this broker implementation shouldn't
		// risk breaking anything that existing brokers handle without complaint.
		
		// If something doesn't explicitly return in these if-then clauses:
		// then it falls to the bottom of this method that always defaults to
		// delegating the method to the original X2broker.

		if (method_getBeanByOid.equals(method)) {
			Class beanType = (Class) args[0];
			String beanOid = (String) args[1];
			if(beanType!=null && beanOid!=null) {
				X2BaseBean bean = dash.getBeanByOid(
						beanType, beanOid);
				if (bean != null)
					return bean;
			}
		} else if (method_getIteratorByQuery.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			QueryByCriteria query = (QueryByCriteria) args[0];
			if(query!=null) {
				active = false;
				try {
					// use THIS broker (the proxy) just so we continue
					// to benefit from potential logging
					QueryIterator returnValue = dash.createQueryIterator(
							(X2Broker) proxy, query);
					return returnValue;
				} finally {
					active = true;
				}
			}
		} else if (method_getBeanByQuery.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			// pass through getIteratorByQuery to benefit from our caching
			try (QueryIterator queryIter = (QueryIterator) invoke(proxy,
					method_getIteratorByQuery, args)) {
				if (queryIter.hasNext())
					return (X2BaseBean) queryIter.next();
				return null;
			}
		} else if (method_getCollectionByQuery.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			// pass through getIteratorByQuery to benefit from our caching
			try (QueryIterator queryIter = (QueryIterator) invoke(proxy,
					method_getIteratorByQuery, args)) {
				Collection<X2BaseBean> result = new ArrayList<>();
				while (queryIter.hasNext()) {
					ThreadUtils.checkInterrupt();

					X2BaseBean bean = (X2BaseBean) queryIter.next();
					result.add(bean);
				}
				return result;
			}
		} else if (method_getGroupedCollectionByQuery1.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			String groupColumn = (String) args[1];
			int initialMapSize = ((Integer) args[2]).intValue();
			if(groupColumn!=null) {
				Object[] newArgs = new Object[] { args[0],
						new String[] { groupColumn },
						new int[] { initialMapSize } };
				return invoke(proxy, method_getGroupedCollectionByQuery2, newArgs);
			}
		} else if (method_getGroupedCollectionByQuery2.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			QueryByCriteria query = (QueryByCriteria) args[0];
			String[] columns = (String[]) args[1];
			int[] mapSizes = (int[]) args[2];
			if(query!=null && columns!=null && mapSizes!=null) {
				return createNestedMap(proxy, query, columns, mapSizes, true);
			}
		} else if (method_getMapByQuery.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			String keyColumn = (String) args[1];
			int initialMapSize = ((Integer) args[2]).intValue() ;
			if(keyColumn!=null) {
				Object[] newArgs = new Object[] { args[0],
						new String[] { keyColumn },
						new int[] { initialMapSize } };
				return invoke(proxy, method_getNestedMapByQuery2, newArgs);
			}
		} else if (method_getNestedMapByQuery1.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			String outerKeyColumn = (String) args[1];
			String innerKeyColumn = (String) args[2];
			int initialOuterMapSize = ((Integer) args[3]).intValue();
			int initialInnerMapSize = ((Integer) args[4]).intValue();
			if(outerKeyColumn!=null && innerKeyColumn!=null) {
				Object[] newArgs = new Object[] {
						args[0],
						new String[] { outerKeyColumn, innerKeyColumn },
						new int[] { initialOuterMapSize, initialInnerMapSize} };
				return invoke(proxy, method_getNestedMapByQuery2, newArgs);
			}
		} else if (method_getNestedMapByQuery2.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			QueryByCriteria query = (QueryByCriteria) args[0];
			String[] columns = (String[]) args[1];
			int[] mapSizes = (int[]) args[2];
			if(query!=null && columns!=null && mapSizes!=null) {
				return createNestedMap(proxy, query, columns, mapSizes, false);
			}
		} else if (method_clearCache.equals(method)) {
			dash.clearAll();
		} else if (method_rollbackTransaction1.equals(method)
				|| method_rollbackTransaction2.equals(method)) {
			dash.clearModifiedBeanTypes();
		} else if ((method_deleteBean.equals(method) || method.getName()
				.startsWith("saveBean")) && args[0] instanceof X2BaseBean) {
			X2BaseBean bean = (X2BaseBean) args[0];
			if(bean!=null) {
				Class t = bean.getClass();
				dash.modifyBeanRecord(t);
			}
		} else if (method_deleteBeanByOid.equals(method)) {
			Class beanType = (Class) args[0];
			String beanOid = (String) args[1];
			if(beanType!=null && beanOid!=null) {
				dash.modifyBeanRecord(beanType);
			}
		} else if (method_deleteByQuery.equals(method)
				|| method_executeUpdateQuery.equals(method)
				|| method_executeInsertQuery.equals(method)) {
			Query query = (Query) args[0];
			if(query!=null) {
				dash.modifyBeanRecord(query.getBaseClass());
			}
		}
		
		Object returnValue = method.invoke(broker, args);
		if (returnValue instanceof X2BaseBean) {
			dash.storeBean((X2BaseBean) returnValue);
		}
		return returnValue;
	}
	
	private boolean logMethod(Level level, Method method, Object[] args,String suffix) {
		Logger log = dash.getLog();
		
		if(log==null || !log.isLoggable(level) || method.getName().equals("getPersistenceKey"))
			return false;
		
		if(suffix!=null) {
			log.log(level, method.getName()+" "+suffix);
		} else if (args == null || args.length == 0) {
			log.log(level, method.getName());
		} else {
			Object[] argsCopy = new Object[args.length];
			for(int a = 0; a<args.length; a++) {
				if(args[a]!=null && args[a].getClass().isArray()) {
					List<Object> list = new LinkedList<>();
					int size = Array.getLength(args[a]);
					for(int i = 0; i<size; i++) {
						Object element = Array.get(args[a], i);
						list.add(element);
					}
					argsCopy[a] = list;
				} else {
					argsCopy[a] = args[a];
				}
			}
			log.log(level, method.getName() + " " + Arrays.asList(argsCopy));
		}
		if(indentHandler!=null)
			indentHandler.increase();
		return true;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object createNestedMap(Object proxy, QueryByCriteria beanQuery,
			String[] columns, int[] mapSizes, boolean useLists)
			throws Throwable {
		// pass through getIteratorByQuery to benefit from our caching
		try (QueryIterator queryIter = (QueryIterator) invoke(proxy,
				method_getIteratorByQuery, new Object[] { beanQuery })) {

			Map map = new HashMap(mapSizes[0]);
			while (queryIter.hasNext()) {
				ThreadUtils.checkInterrupt();

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