package org.abc.dash;

import java.lang.Thread.UncaughtExceptionHandler;
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
			method_getDash = BrokerDash.class
					.getMethod("getDash");
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
			method_getBeanByOid = X2Broker.class.getMethod("getBeanByOid", Class.class, String.class);
			initialized = true;
		} catch (Exception e) {
			constructionException = new RuntimeException("An error occurred initializing the Dash cache architecture, so it is being deactivated.", e);
		}
	}
	
	static class IndentionHandler extends Handler {
		
		int spaces = 2;

		@Override
		public void publish(LogRecord record) {
			StringBuilder sb = new StringBuilder();
			for(int a = 0; a<spaces; a++) {
				sb.append(" ");
			}
			String msg = record.getMessage();
			sb.append(msg);
			record.setMessage(sb.toString());
		}

		@Override
		public void flush() {}

		@Override
		public void close() throws SecurityException {}

		public void increase() {
			spaces += 2;
		}
		
	}

	X2Broker broker;
	boolean active = initialized;
	Dash dash;

	DashInvocationHandler(X2Broker broker,
			Dash dash) {
		if(constructionException!=null)
			throw constructionException;
		Objects.requireNonNull(broker);
		Objects.requireNonNull(dash);
		this.broker = broker;
		this.dash = dash;
	}

	protected void handleException(Exception e) {
		Exception e2 = new Exception("An error occurred using the Dash cache architecture, so it is being deactivated.", e);
		UncaughtExceptionHandler ueh = dash.getUncaughtExceptionHandler();
		if(ueh!=null) {
			ueh.uncaughtException(Thread.currentThread(), e2);
		}
		
		Logger log = dash.getLog();
		if(log!=null && log.isLoggable(Level.WARNING))
			log.warning(LoggerUtils.convertThrowableToString(e2));
		
		active = false;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		Logger log = dash.getLog();

		// handle methods unique to the BrokerDash interface:

		if (method_getDash.equals(method)) {
			return dash;
		} else if(method.getName().equals("setDashActive")) {
			if(log!=null && log.isLoggable(Level.FINE)) {
				log.fine("setDashActive "+args[0]);
			}
			active = ((Boolean)args[0]).booleanValue();
			return Void.TYPE;
		} else if(method.getName().equals("isDashActive")) {
			return active;
		}
		
		IndentionHandler indentHandler = indentLog(log);
		try {
			// if possible: intercept methods using our caching model/layer

			if (active) {
				try {
					return invokeCached(proxy, method, args, log, indentHandler);
				} catch (Exception e) {
					handleException(e);
				}
			}

			// ... if that fails: invoke the method without our caching
			// model/layer:

			if(log!=null && log.isLoggable(Level.FINEST) && !method.getName().equals("getPersistenceKey")) {
				if(args==null || args.length==0) {
					log.finest(method.getName());
				} else {
					log.finest(method.getName()+" "+Arrays.asList(args));
				}
			}
			
			long t = System.currentTimeMillis();
			Object returnValue = method.invoke(broker, args);
			t = System.currentTimeMillis() - t;
			if(t>10 && log!=null && log.isLoggable(Level.FINEST) && !method.getName().equals("getPersistenceKey")) {
				log.finest(method.getName()+" (ended)");
			}
			
			return returnValue;
		} finally {
			if(indentHandler!=null)
				log.removeHandler(indentHandler);
		}
	}

	private IndentionHandler indentLog(Logger log) {
		if(log==null)
			return null;
		//insert our handler as the zero-eth handler so it grabs messages before anyone else
		IndentionHandler indentHandler = new IndentionHandler();
		Handler[] oldHandlers = log.getHandlers();
		for(int a = 0; a<oldHandlers.length; a++) {
			log.removeHandler(oldHandlers[a]);
		}
		log.addHandler(indentHandler);
		for(int a = 0; a<oldHandlers.length; a++) {
			log.addHandler(oldHandlers[a]);
		}
		return indentHandler;
	}

	@SuppressWarnings({ "rawtypes" })
	protected Object invokeCached(Object proxy, Method method, Object[] args,Logger log,IndentionHandler indention)
			throws Throwable {
		if(method_getBeanByOid.equals(method)) {
			if(log!=null && log.isLoggable(Level.FINER)) {
				log.finer(method.getName()+" "+Arrays.asList(args));
			}
			indention.increase();
			X2BaseBean bean = dash.getBeanByOid(broker.getPersistenceKey(), (Class) args[0], (String) args[1]);
			if(bean!=null)
				return bean;
		} else if (method_getIteratorByQuery.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			if(log!=null && log.isLoggable(Level.FINER)) {
				log.finer(method.getName()+" "+Arrays.asList(args));
			}
			indention.increase();
			QueryByCriteria query = (QueryByCriteria) args[0];
			active = false;
			try {
				QueryIterator returnValue = dash.createQueryIterator( (X2Broker) proxy, query);
				return returnValue;
			} finally {
				active = true;
			}
		} else if (method_getBeanByQuery.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			if(log!=null && log.isLoggable(Level.FINER)) {
				log.finer(method.getName()+" "+Arrays.asList(args));
			}
			indention.increase();
			// pass through getIteratorByQuery to benefit from our caching
			try (QueryIterator queryIter = (QueryIterator) invoke(proxy,
					method_getIteratorByQuery, args)) {
				if (queryIter.hasNext())
					return (X2BaseBean) queryIter.next();
				return null;
			}
		} else if (method_getCollectionByQuery.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			if(log!=null && log.isLoggable(Level.FINER)) {
				log.finer(method.getName()+" "+Arrays.asList(args));
			}
			indention.increase();
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
				&& Dash.isBeanQuery(args[0])) {
			Object[] newArgs = new Object[] { args[0],
					new String[] { (String) args[1] },
					new int[] { ((Integer) args[2]).intValue() } };
			return invoke(proxy, method_getGroupedCollectionByQuery2,
					newArgs);
		} else if (method_getGroupedCollectionByQuery2.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			if(log!=null && log.isLoggable(Level.FINER)) {
				log.finer(method.getName()+" "+Arrays.asList(args));
			}
			indention.increase();
			QueryByCriteria query = (QueryByCriteria) args[0];
			String[] columns = (String[]) args[1];
			int[] mapSizes = (int[]) args[2];
			return createNestedMap(proxy, query, columns, mapSizes, true);
		} else if (method_getMapByQuery.equals(method)
				&& Dash.isBeanQuery(args[0]) ) {
			Object[] newArgs = new Object[] { args[0],
					new String[] { (String) args[1] },
					new int[] { ((Integer) args[2]).intValue() } };
			return invoke(proxy, method_getNestedMapByQuery2, newArgs);
		} else if (method_getNestedMapByQuery1.equals(method)
				&& Dash.isBeanQuery(args[0])) {
			Object[] newArgs = new Object[] {
					args[0],
					new String[] { (String) args[1], (String) args[2] },
					new int[] { ((Integer) args[3]).intValue(),
							((Integer) args[4]).intValue() } };
			return invoke(proxy, method_getNestedMapByQuery2, newArgs);
		} else if (method_getNestedMapByQuery2.equals(method)
				&& Dash.isBeanQuery(args[0]) ) {
			if(log!=null && log.isLoggable(Level.FINER)) {
				log.finer(method.getName()+" "+Arrays.asList(args));
			}
			indention.increase();
			QueryByCriteria query = (QueryByCriteria) args[0];
			String[] columns = (String[]) args[1];
			int[] mapSizes = (int[]) args[2];
			return createNestedMap(proxy, query, columns, mapSizes, false);
		} else if (method_clearCache.equals(method)) {
			dash.clearAll();
		} else if(method_rollbackTransaction1.equals(method)
				|| method_rollbackTransaction2.equals(method)) {
			dash.clearModifiedBeanTypes();
		} else if ((method_deleteBean.equals(method) || method.getName()
				.startsWith("saveBean")) && args[0] instanceof X2BaseBean) {
			X2BaseBean bean = (X2BaseBean) args[0];
			Class t = bean.getClass();
			dash.modifyBeanRecord(t);
		} else if (method_deleteBeanByOid.equals(method)) {
			Class beanType = (Class) args[0];
			dash.modifyBeanRecord(beanType);
		} else if (method_deleteByQuery.equals(method)
				|| method_executeUpdateQuery.equals(method)
				|| method_executeInsertQuery.equals(method)) {
			Query query = (Query) args[0];
			dash.modifyBeanRecord(query.getBaseClass());
		}

		if(log!=null && log.isLoggable(Level.FINEST) && !method.getName().equals("getPersistenceKey")) {
			if(args==null || args.length==0) {
				log.finest(method.getName());
			} else {
				log.finest(method.getName()+" "+Arrays.asList(args));
			}
		}
		Object returnValue = method.invoke(broker, args);
		if(returnValue instanceof X2BaseBean) {
			dash.storeBean( (X2BaseBean) returnValue );
		}
		return returnValue;
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