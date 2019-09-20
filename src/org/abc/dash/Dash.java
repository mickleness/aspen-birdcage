package org.abc.dash;

import java.io.IOException;
import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import org.abc.tools.Tool;
import org.abc.util.BasicEntry;
import org.abc.util.OrderByComparator;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.ojb.broker.Identity;
import org.apache.ojb.broker.PersistenceBroker;
import org.apache.ojb.broker.metadata.FieldHelper;
import org.apache.ojb.broker.query.BetweenCriteria;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.LikeCriteria;
import org.apache.ojb.broker.query.Query;
import org.apache.ojb.broker.query.QueryByCriteria;
import org.apache.ojb.broker.query.ValueCriteria;

import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.InsertQuery;
import com.follett.fsc.core.framework.persistence.UpdateQuery;
import com.follett.fsc.core.framework.persistence.X2ObjectCache;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.tools.procedures.ProcedureJavaSource;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.pump.data.operator.And;
import com.pump.data.operator.EqualTo;
import com.pump.data.operator.GreaterThan;
import com.pump.data.operator.In;
import com.pump.data.operator.LesserThan;
import com.pump.data.operator.Like;
import com.pump.data.operator.Not;
import com.pump.data.operator.Operator;
import com.pump.data.operator.OperatorContext;
import com.pump.data.operator.Or;
import com.pump.text.WildcardPattern;
import com.pump.util.Cache;
import com.pump.util.Cache.CachePool;
import com.x2dev.utils.StringUtils;

/**
 * This class contains the Dash architecture.
 * <p>
 * This contains a series a public inner classes/interfaces.
 * In an ideal world: all of these inner classes would be declared
 * in a separate file. But given the constraints of Aspen tools: these
 * are bundled here so they can be easily referenced after setting up
 * a single helper tool.
 */
@Tool(name = "Dash Caching Model", id = "DASH-CACHE")
public class Dash extends ProcedureJavaSource {
	private static final long serialVersionUID = 1L;
	
	/**
	 * A BrokerDash is an X2Broker that caches some of its operations.
	 */
	public interface BrokerDash extends X2Broker {

		/**
		 * This is an OperatorContext that connects Operators to X2BaseBeans.
		 */
		OperatorContext CONTEXT = new OperatorContext() {

			@Override
			public Object getValue(Object dataSource, String attributeName) {

				abstract class Function {
					public abstract Object evaluate(Object input);
				}

				class Parser {
					LinkedList<Function> functions = new LinkedList<>();
					String attributeName;

					public Parser(String input) {
						while (true) {
							if (input.startsWith("upper(") && input.endsWith(")")) {
								functions.add(new Function() {

									@Override
									public Object evaluate(Object input) {
										if (input == null)
											return null;
										return ((String) input).toUpperCase();
									}

								});
								input = input.substring("upper(".length(),
										input.length() - 1);
							} else if (input.startsWith("ISNUMERIC(")
									&& input.endsWith(")")) {
								functions.add(new Function() {

									@Override
									public Object evaluate(Object input) {
										if (input == null)
											return null;
										return StringUtils
												.isNumeric((String) input);
									}

								});
								input = input.substring("ISNUMERIC(".length(),
										input.length() - 1);
							} else {
								attributeName = input;
								return;
							}
						}
					}

				}

				Parser parser = new Parser(attributeName);

				Object value;
				try {
					value = PropertyUtils.getProperty(dataSource,
							parser.attributeName);
				} catch (IllegalAccessException | InvocationTargetException
						| NoSuchMethodException e) {
					throw new RuntimeException("An error occurred retrieving \""
							+ attributeName + "\" from " + dataSource, e);
				}
				Iterator<Function> iter = parser.functions.descendingIterator();
				while (iter.hasNext()) {
					value = iter.next().evaluate(value);
				}
				return value;
			}

		};

		/**
		 * Multiple BrokerDashes may share the same BrokerDashSharedResource.
		 */
		BrokerDashSharedResource getSharedResource();
	}

	/**
	 * This converts X2Brokers into BrokerDashes.
	 * <p>
	 * All BrokerDashes created from the same BrokerDashFactory share the same
	 * BrokerDashSharedResource / CachePool.
	 */
	public static class BrokerDashFactory {

		static class DashInvocationHandler implements InvocationHandler {
			static RuntimeException constructionException;
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
			static Method method_getBeanByOid;
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
					method_getBeanByOid = X2Broker.class.getMethod("getBeanByOid", Class.class, String.class);
					initialized = true;
				} catch (Exception e) {
					constructionException = new RuntimeException("An error occurred initializing the Dash cache architecture, so it is being deactivated.", e);
				}
			}

			X2Broker broker;
			boolean active = initialized;
			BrokerDashSharedResource sharedResource;

			DashInvocationHandler(X2Broker broker,
					BrokerDashSharedResource sharedResource) {
				if(constructionException!=null)
					throw constructionException;
				Objects.requireNonNull(broker);
				Objects.requireNonNull(sharedResource);
				this.broker = broker;
				this.sharedResource = sharedResource;
			}

			protected void handleException(Exception e) {
				Exception e2 = new Exception("An error occurred using the Dash cache architecture, so it is being deactivated.", e);
				UncaughtExceptionHandler ueh = sharedResource.getUncaughtExceptionHandler();
				if(ueh!=null) {
					ueh.uncaughtException(Thread.currentThread(), e2);
				}
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
				if(method_getBeanByOid.equals(method)) {
					// we don't benefit from intercepting this method, but it is helpful in debugging logs
					// to confirm which methods we're able to absorb without consulting the parent broker
					X2BaseBean bean = getBeanFromGlobalCache(broker.getPersistenceKey(), (Class) args[0], (String) args[1]);
					if(bean!=null)
						return bean;
				} else if (method_getIteratorByQuery.equals(method)
						&& isBeanQuery(args[0])) {
					QueryByCriteria query = (QueryByCriteria) args[0];
					QueryIterator returnValue = sharedResource.createDashIterator(broker, query);
					return returnValue;
				} else if (method_getBeanByQuery.equals(method)
						&& isBeanQuery(args[0])) {
					// pass through getIteratorByQuery to benefit from our caching
					try (QueryIterator queryIter = (QueryIterator) invoke(proxy,
							method_getIteratorByQuery, args)) {
						if (queryIter.hasNext())
							return (X2BaseBean) queryIter.next();
						return null;
					}
				} else if (method_getCollectionByQuery.equals(method)
						&& isBeanQuery(args[0])) {
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
						&& isBeanQuery(args[0])) {
					Object[] newArgs = new Object[] { args[0],
							new String[] { (String) args[1] },
							new int[] { ((Integer) args[2]).intValue() } };
					return invoke(proxy, method_getGroupedCollectionByQuery2,
							newArgs);
				} else if (method_getGroupedCollectionByQuery2.equals(method)
						&& isBeanQuery(args[0])) {
					QueryByCriteria query = (QueryByCriteria) args[0];
					String[] columns = (String[]) args[1];
					int[] mapSizes = (int[]) args[2];
					return createNestedMap(proxy, query, columns, mapSizes, true);
				} else if (method_getMapByQuery.equals(method)
						&& isBeanQuery(args[0]) ) {
					Object[] newArgs = new Object[] { args[0],
							new String[] { (String) args[1] },
							new int[] { ((Integer) args[2]).intValue() } };
					return invoke(proxy, method_getNestedMapByQuery2, newArgs);
				} else if (method_getNestedMapByQuery1.equals(method)
						&& isBeanQuery(args[0])) {
					Object[] newArgs = new Object[] {
							args[0],
							new String[] { (String) args[1], (String) args[2] },
							new int[] { ((Integer) args[3]).intValue(),
									((Integer) args[4]).intValue() } };
					return invoke(proxy, method_getNestedMapByQuery2, newArgs);
				} else if (method_getNestedMapByQuery2.equals(method)
						&& isBeanQuery(args[0]) ) {
					QueryByCriteria query = (QueryByCriteria) args[0];
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
					sharedResource.clearCache(t);
				} else if (method_deleteBeanByOid.equals(method)) {
					Class beanType = (Class) args[0];
					sharedResource.clearCache(beanType);
				} else if (method_deleteByQuery.equals(method)
						|| method_executeUpdateQuery.equals(method)
						|| method_executeInsertQuery.equals(method)) {
					Query query = (Query) args[0];
					sharedResource.clearCache(query.getBaseClass());
				}

				return method.invoke(broker, args);
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

	/**
	 * This catalogs the number of times certain uncaching operations have been
	 * invoked.
	 */
	public static class CacheResults implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * This describes an attempt to uncache a bean query.
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
			HIT_FROM_SPLIT,
			/**
			 * This indicates caching wasn't attempted because a Criteria couldn't be converted to an Operator.
			 * (This is probably because a criteria contained a subquery, or some other unsupported feature).
			 */
			SKIP_UNSUPPORTED;
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
	
	/**
	 * This converts org.apache.ojb.broker.query.Criterias into 
	 * com.pump.data.operator.Operators (and back again).
	 *
	 */
	public static class CriteriaToOperatorConverter {

		static WildcardPattern.Format sqlFormat = new WildcardPattern.Format();
		static {
			sqlFormat.closeBracketCharacter = null;
			sqlFormat.openBracketCharacter = null;
			sqlFormat.escapeCharacter = LikeCriteria.getEscapeCharacter();
			sqlFormat.questionMarkWildcard = '_';
			sqlFormat.starWildcard = '%';
		}

		/**
		 * Convert an Operator into a Criteria.
		 */
		public static Criteria createCriteria(Operator operator) {
			Criteria c = new Criteria();
			if (operator instanceof Not) {
				Operator sub = (Operator) operator.getOperand(0);
				if (sub instanceof EqualTo) {
					EqualTo e = (EqualTo) sub;
					if (e.getValue() == null) {
						c.addNotNull(e.getAttribute());
					} else {
						c.addNotEqualTo(e.getAttribute(), e.getValue());
					}
				} else if (sub instanceof GreaterThan) {
					GreaterThan gt = (GreaterThan) sub;
					c.addLessOrEqualThan(gt.getAttribute(), gt.getValue());
				} else if (sub instanceof LesserThan) {
					LesserThan lt = (LesserThan) sub;
					c.addGreaterOrEqualThan(lt.getAttribute(), lt.getValue());
				} else if (sub instanceof In) {
					In i = (In) sub;
					c.addNotIn(i.getAttribute(), i.getValue());
				} else if (sub instanceof Like) {
					Like l = (Like) sub;
					c.addNotLike(l.getAttribute(), l.getValue().getPatternText());
				} else if (sub instanceof And) {
					And and = (And) sub;
					for (int a = 0; a < and.getOperandCount(); a++) {
						Operator x = and.getOperand(a);
						if (x instanceof Not) {
							x = (Operator) x.getOperand(0);
						} else {
							x = new Not(x);
						}
						Criteria negatedOr = createCriteria(x);
						c.addOrCriteria(negatedOr);
					}
				} else if (sub instanceof Or) {
					Or or = (Or) sub;
					for (int a = 0; a < or.getOperandCount(); a++) {
						Operator x = or.getOperand(a);
						if (x instanceof Not) {
							x = (Operator) x.getOperand(0);
						} else {
							x = new Not(x);
						}
						Criteria negatedAnd = createCriteria(x);
						c.addAndCriteria(negatedAnd);
					}
				} else {
					throw new IllegalArgumentException("Unsupporter operator: "
							+ operator.getClass().getName() + " \"" + operator
							+ "\"");
				}
			} else if (operator instanceof EqualTo) {
				EqualTo e = (EqualTo) operator;
				if (e.getValue() == null) {
					c.addIsNull(e.getAttribute());
				} else {
					c.addEqualTo(e.getAttribute(), e.getValue());
				}
			} else if (operator instanceof GreaterThan) {
				GreaterThan gt = (GreaterThan) operator;
				c.addGreaterThan(gt.getAttribute(), gt.getValue());
			} else if (operator instanceof LesserThan) {
				LesserThan lt = (LesserThan) operator;
				c.addLessThan(lt.getAttribute(), lt.getValue());
			} else if (operator instanceof In) {
				In i = (In) operator;
				c.addIn(i.getAttribute(), i.getValue());
			} else if (operator instanceof Like) {
				Like l = (Like) operator;
				c.addLike(l.getAttribute(), l.getValue().getPatternText());
			} else if (operator instanceof And) {
				And and = (And) operator;
				for (int a = 0; a < and.getOperandCount(); a++) {
					Criteria andC = createCriteria(and.getOperand(a));
					c.addAndCriteria(andC);
				}
			} else if (operator instanceof Or) {
				Or or = (Or) operator;
				for (int a = 0; a < or.getOperandCount(); a++) {
					Criteria orC = createCriteria(or.getOperand(a));
					c.addOrCriteria(orC);
				}
			} else {
				throw new IllegalArgumentException("Unsupporter operator: "
						+ operator.getClass().getName() + " \"" + operator + "\"");
			}
			return c;
		}

		/**
		 * Convert a Criteria into an Operator.
		 * <p>
		 * This may throw an exception if this uses a criteria feature that is not
		 * supported by Operators.
		 */
		@SuppressWarnings("rawtypes")
		public static Operator createOperator(Criteria criteria) {
			Enumeration e = criteria.getElements();
			Operator current = null;
			while (e.hasMoreElements()) {
				Object z = e.nextElement();
				Operator op = null;
				if (z instanceof ValueCriteria) {
					ValueCriteria vc = (ValueCriteria) z;
					BetweenCriteria bc = vc instanceof BetweenCriteria ? (BetweenCriteria) vc
							: null;
					String clause = vc.getClause();
					// these Strings are copied and pasted from protected constants
					// in SelectionCriteria
					switch (clause) {
					case " = ":
						op = new EqualTo((String) vc.getAttribute(),
								(Comparable) vc.getValue());
						break;
					case " <> ":
						op = new Not(new EqualTo((String) vc.getAttribute(),
								(Comparable) vc.getValue()));
						break;
					case " > ":
						op = new GreaterThan((String) vc.getAttribute(),
								(Comparable) vc.getValue());
						break;
					case " <= ":
						op = new Not(new GreaterThan((String) vc.getAttribute(),
								(Comparable) vc.getValue()));
						break;
					case " < ":
						op = new LesserThan((String) vc.getAttribute(),
								(Comparable) vc.getValue());
						break;
					case " >= ":
						op = new Not(new LesserThan((String) vc.getAttribute(),
								(Comparable) vc.getValue()));
						break;
					case " LIKE ":
						op = new Like((String) vc.getAttribute(),
								new WildcardPattern((String) vc.getValue(),
										sqlFormat));
						break;
					case " NOT LIKE ":
						op = new Not(new Like((String) vc.getAttribute(),
								new WildcardPattern((String) vc.getValue(),
										sqlFormat)));
						break;
					case " IS NULL ":
						op = new EqualTo((String) vc.getAttribute(), null);
						break;
					case " IS NOT NULL ":
						op = new Not(new EqualTo((String) vc.getAttribute(), null));
						break;
					case " BETWEEN ":
						op = new And(new GreaterThan((String) bc.getAttribute(),
								(Comparable) bc.getValue()), new LesserThan(
								(String) bc.getAttribute(),
								(Comparable) bc.getValue2()));
						break;
					case " NOT BETWEEN ":
						op = new Or(new LesserThan((String) bc.getAttribute(),
								(Comparable) bc.getValue()), new GreaterThan(
								(String) bc.getAttribute(),
								(Comparable) bc.getValue2()));
						break;
					case " IN ":
						op = In.create((String) vc.getAttribute(),
								(Collection<?>) vc.getValue());
						break;
					case " NOT IN ":
						op = new Not(In.create((String) vc.getAttribute(),
								(Collection<?>) vc.getValue()));
						break;
					}

					if (op == null)
						throw new IllegalArgumentException(
								"Unsupported criteria element: " + z + " ("
										+ z.getClass().getName() + ")");

					if (current == null) {
						current = op;
					} else {
						current = new And(current, op);
					}
				} else if (z instanceof Criteria) {
					Criteria c2 = (Criteria) z;
					op = createOperator((Criteria) z);
					if (current == null) {
						current = op;
					} else if (c2.getType() == Criteria.AND) {
						current = new And(current, op);
					} else if (c2.getType() == Criteria.OR) {
						current = new Or(current, op);
					} else {
						throw new IllegalArgumentException(
								"Unsupported Criteria type " + c2.getType());
					}
				} else {
					if (op == null)
						throw new IllegalArgumentException(
								"Unsupported criteria element: " + z + " ("
										+ z.getClass().getName() + ")");
				}
			}
			return current;
		}
	}
	
	/**
	 * This iterates over a series of X2BaseBeans.
	 * <p>
	 * This may pull data from two sources: a predefined collection of
	 * beans and/or a QueryIterator.
	 */
	public static class QueryIteratorDash extends QueryIterator<X2BaseBean> {
		
		/**
		 * This listener is notified when an iterator closes.
		 */
		static interface CloseListener {
			/**
			 * This method is invoked when an iterator closes.
			 * 
			 * @param returnCount the number of times this iterator produced results.
			 * @param hasNext whether this iterator's {@link Iterator#hasNext()} method
			 * indicated there were results remaining when this iterator was closed.
			 */
			public void closedIterator(int returnCount,boolean hasNext);
		}
		
		/**
		 * This is never null, although it may be empty.
		 */
		protected Collection<X2BaseBean> beans;
		
		/**
		 * This may always be null, or it may become null on exhaustion.
		 */
		protected QueryIterator<X2BaseBean> queryIterator;
		
		protected PersistenceKey persistenceKey;
		protected List<CloseListener> closeListeners = new ArrayList<>();
		
		/**
		 * The number of successful invocations of {@link #next()}.
		 */
		protected int nextCounter = 0;
		
		/**
		 * Create an iterator that will walk through a collection of beans.
		 */
		public QueryIteratorDash(PersistenceKey persistenceKey,Collection<X2BaseBean> beans) {
			this(persistenceKey, beans, null);
		}

		/**
		 * Create an iterator that will walk through a collection of beans and a QueryIterator.
		 * <p>
		 * Every time a new bean is requested: if possible we pull a bean from the QueryIterator
		 * and add it to the collection of beans. Then we return the first bean from the collection of beans.
		 * <p>
		 * So if the collection is a List, this approach will generate a FIFO iterator.
		 * If the collection is a SortedSet, then this approach will merge the QueryIterator
		 * with the SortedSet. (This assumes the QueryIterator will return results using the
		 * same order-by rules that the SortedSet follows.)
		 * 
		 * @param persistenceKey the PersistenceKey associated with this iterator
		 * @param beans an optional collection of beans to walk through. This may be null.
		 * @param queryIterator the optional QueryIterator to walk through. This may be null.
		 */
		public QueryIteratorDash(PersistenceKey persistenceKey, Collection<X2BaseBean> beans, QueryIterator<X2BaseBean> queryIterator) {
			Objects.requireNonNull(persistenceKey);
			this.beans = beans==null ? new LinkedList<X2BaseBean>() : beans;
			this.queryIterator = queryIterator;
			this.persistenceKey = persistenceKey;
		}
		
		/**
		 * Add a CloseListener that will be notified when this iterator is closed.
		 */
		public void addCloseListener(CloseListener l) {
			closeListeners.add(l);
		}
		
		/**
		 * Remove a CloseListener.
		 */
		public void removeCloseListener(CloseListener l) {
			closeListeners.remove(l);
		}

		@Override
		protected void finalize() {
			close();
		}

		@Override
		public void close() {
			boolean hasNext = hasNext();
			
			if(queryIterator!=null)
				queryIterator.close();
			queryIterator = null;
			beans.clear();
			
			for(CloseListener listener : closeListeners.toArray(new CloseListener[closeListeners.size()])) {
				listener.closedIterator(nextCounter, hasNext);
			}
		}

		@SuppressWarnings("rawtypes")
		@Override
		protected Iterator getIterator(PersistenceBroker arg0, Query arg1) {
			throw new UnsupportedOperationException();
		}

		@Override
		public X2BaseBean next() {
			if(queryIterator!=null) {
				if(queryIterator.hasNext()) {
					X2BaseBean bean = queryIterator.next();
					beans.add(bean);
				}
				if(!queryIterator.hasNext()) {
					queryIterator.close();
					queryIterator = null;
				}
			}
			
			if(beans.size()==0)
				return null;
			
			Iterator<X2BaseBean> beanIter = beans.iterator();
			X2BaseBean returnValue = beanIter.next();
			beanIter.remove();
			nextCounter++;
			return returnValue;
		}

		@Override
		public boolean hasNext() {
			if(!beans.isEmpty())
				return true;
			if(queryIterator!=null && queryIterator.hasNext())
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
	 * This monitors a few properties about a template query.
	 * <p>
	 * Here a "template query" means a query stripped of specific values. For example
	 * you query for records where "A==1 || B==true", then the template of that query
	 * resembles "A==? || B==?". Two different queries that use the same fields but
	 * different values match the same template.
	 * 
	 */
	public static class TemplateQueryProfile implements QueryIteratorDash.CloseListener, Serializable {
		private static final long serialVersionUID = 1L;

		protected int ctr = 0;
		protected int maxReturnCount = 0;
		protected double averageReturnCount = 0;
		protected CacheResults results = new CacheResults();
		
		/**
		 * The total number of times a query matching this template has been issued.
		 */
		public synchronized int getCounter() {
			return ctr;
		}
		
		/**
		 * The average number of beans a QueryIterator iterated over.
		 * <p>
		 * In most cases this is synonymous with "the number of beans a query produces",
		 * but if the iterator is abandoned prematurely then these two values may be
		 * different.
		 */
		public synchronized double getAverageReturnCount() {
			return averageReturnCount;
		}
		
		/**
		 * The maximum number of beans a QueryIterator iterated over.
		 */
		public synchronized int getMaxReturnCount() {
			return maxReturnCount;
		}
		
		/**
		 * Return CacheResults associated with this template.
		 */
		public synchronized CacheResults getResults() {
			return results;
		}

		/**
		 * This object is attached to every QueryIteratorDash a
		 * BrokerDashSharedResource produces, and when that iterator is closed
		 * this object's statistics are updated.
		 */
		@Override
		public synchronized void closedIterator(int returnCount, boolean hasNext) {
			maxReturnCount = Math.max(maxReturnCount, returnCount);
			double total = averageReturnCount * ctr;
			total += returnCount;
			ctr++;
			averageReturnCount = total / ((double)ctr);
		}

		private void readObject(java.io.ObjectInputStream in) throws IOException,
				ClassNotFoundException {
			int version = in.readInt();
			if (version == 0) {
				ctr = in.readInt();
				maxReturnCount = in.readInt();
				averageReturnCount = in.readDouble();
				results = (CacheResults) in.readObject();
			} else {
				throw new IOException("Unsupported internal version: " + version);
			}
		}

		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.writeInt(0);
			out.writeInt(ctr);
			out.writeInt(maxReturnCount);
			out.writeDouble(averageReturnCount);
			out.writeObject(results);
		}

		@Override
		public String toString() {
			return "TemplateQueryProfile[ ctr="+getCounter()+
					", averageReturnCount="+getAverageReturnCount()+
					", maxReturnCount="+getMaxReturnCount()+
					", results="+getResults()+"]";
		}
	}
	
	/**
	 * This maintains caches of oids for specific queries.
	 * <p>
	 * The method {@link #createDashIterator(X2Broker, QueryByCriteria)} consults and updates
	 * these caches and may return an iterator that doesn't consult the database at all
	 * or that consults the database for a subset of the original query.
	 * <p>
	 * If the same BrokerDashSharedResource is shared across multiple threads it draws
	 * on the same cache.
	 * <p>
	 * You're encouraged to subclass this method to override the <code>is</code> methods
	 * that are used to decide when/how to cache data.
	 */
	public static class BrokerDashSharedResource {
		
		protected CachePool cachePool;
		protected Cache<ProfileKey, TemplateQueryProfile> profiles;
		protected CacheResults cacheResults = new CacheResults();
		protected Map<Class<?>, Cache<CacheKey, List<String>>> cacheByBeanType = new HashMap<>();

		/**
		 * Create a new BrokerDashSharedResource.
		 * 
		 * @param maxCacheSize the maximum number of elements that can exist in the cache.
		 * @param maxCacheDuration the maximum duration (in milliseconds) any entry
		 * can exist in the cache.
		 */
		public BrokerDashSharedResource(int maxCacheSize, long maxCacheDuration) {
			this(new CachePool(maxCacheSize, maxCacheDuration, -1));
		}
		/**
		 * Create a new BrokerDashSharedResource.
		 * 
		 * @param cachePool the CachePool used to maintain all cached data.
		 */
		public BrokerDashSharedResource(CachePool cachePool) {
			Objects.requireNonNull(cachePool);
			this.cachePool = cachePool;
			profiles = new Cache<>(cachePool);
		}

		/**
		 * Return the Cache associated with a given bean class.
		 * 
		 * @param beanClass the type of bean to fetch the cache for.
		 * @param createIfMissing if true then may create a new Cache if it doesn't already exist. 
		 * If false then this method may return null.
		 */
		protected Cache<CacheKey, List<String>> getCache(Class<?> beanClass, boolean createIfMissing) {
			synchronized(cacheByBeanType) {
				Cache<CacheKey, List<String>> cache = cacheByBeanType.get(beanClass);
				if(cache==null && createIfMissing) {
					cache = new Cache<>(cachePool);
					cacheByBeanType.put(beanClass, cache);
				}
				return cache;
			}
		}

		/**
		 * Create a QueryIteratorDash for a QueryByCriteria.
		 * <p>
		 * In an ideal case: this will use cached data to completely avoid making a
		 * database query.
		 * <p>
		 * This method should never issue more than one database query. There are 3
		 * database queries this can issue:
		 * <ul><li>The original incoming query as-is.</li>
		 * <li>A query to retrieve beans based on oids. If this caching layer was able to identify
		 * the exact oids we need, but those beans are no longer in Aspen's cache: a query based
		 * on the oids should be more efficient.</li>
		 * <li>A query to retrieve a subset of the original query. In this case we were able to
		 * split the original query into smaller pieces, and some of those pieces we could uncache
		 * and others we could not.</li></ul>
		 */
		public final QueryIteratorDash createDashIterator(X2Broker broker,QueryByCriteria beanQuery) {
			if(!isBeanQuery(beanQuery))
				throw new IllegalArgumentException("This query is not for an X2BaseBean: "+beanQuery);
			Operator operator;
			try {
				operator = CriteriaToOperatorConverter
					.createOperator(beanQuery.getCriteria());
			} catch(Exception e) {
				//this Criteria can't be converted to an Operator, so we should give up:

				QueryIterator iter = broker.getIteratorByQuery(beanQuery);
				QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), null, iter);
				cacheResults.increment(CacheResults.Type.SKIP_UNSUPPORTED);
				return dashIter;
			}
			
			Operator template = operator.getTemplateOperator();
			ProfileKey profileKey = new ProfileKey(template, beanQuery.getBaseClass());

			TemplateQueryProfile profile;
			synchronized(profiles) {
				profile = profiles.get(profileKey);
				if(profile==null) {
					profile = new TemplateQueryProfile();
					profiles.put(profileKey, profile);
				}
			}
			
			Map.Entry<QueryIteratorDash,CacheResults.Type> results = doCreateDashIterator(broker, beanQuery, operator, profile);
			results.getKey().addCloseListener(profile);
			profile.getResults().increment(results.getValue());
			cacheResults.increment(results.getValue());
			return results.getKey();
		}
		
		/**
		 * Return the overall cache results of all BeanQueries that passed through this object.
		 */
		public CacheResults getCacheResults() {
			return cacheResults;
		}

		/**
		 * Create a QueryIteratorDash for the given query.
		 * 
		 * @return the iterator and the way to classify this request in CacheResults objects.
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		protected Map.Entry<QueryIteratorDash,CacheResults.Type> doCreateDashIterator(X2Broker broker, QueryByCriteria beanQuery, Operator operator, TemplateQueryProfile profile) {
			OrderByComparator orderBy = new OrderByComparator(false,
					beanQuery.getOrderBy());
			
			if(!isCaching(orderBy, profile, beanQuery)) {
				QueryIterator iter = broker.getIteratorByQuery(beanQuery);
				QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), null, iter);
				return new BasicEntry<>(dashIter, CacheResults.Type.SKIP);
			}
			
			Cache<CacheKey, List<String>> cache = getCache(beanQuery.getBaseClass(), true);
			CacheKey cacheKey = new CacheKey(operator, orderBy, beanQuery.isDistinct());
			List<String> beanOids = cache.get(cacheKey);
			
			if (beanOids != null) {
				List<X2BaseBean> beans = getBeansFromGlobalCache(broker.getPersistenceKey(), beanQuery.getBaseClass(), beanOids);
				if(beans!=null) {
					// This is our ideal case: we know the complete query results
					QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), beans);
					return new BasicEntry<>(dashIter, CacheResults.Type.HIT);
				}
				
				// We know the exact oids, but those beans aren't in Aspen's cache 
				// anymore. We can at least rewrite the query:
				
				Criteria oidCriteria = new Criteria();
				oidCriteria.addIn(X2BaseBean.COL_OID, beanOids);
				beanQuery = cloneBeanQuery(beanQuery, oidCriteria);

				QueryIterator iter = broker.getIteratorByQuery(beanQuery);
				QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), null, iter);
				return new BasicEntry<>(dashIter, CacheResults.Type.REDUCED_QUERY_TO_OIDS);
			}
			
			// we couldn't retrieve the entire query results from our cache

			Operator canonicalOperator = operator.getCanonicalOperator();
			Collection<Operator> splitOperators = canonicalOperator.split();

			if(splitOperators.size() <= 1 || !isCachingSplit(orderBy, profile, operator)) {
				// this is the simple scenario (no splitting)
				Collection<X2BaseBean> beansToReturn = new LinkedList<>();
				QueryIterator iter = broker.getIteratorByQuery(beanQuery);
				int ctr = 0;
				int maxSize = getMaxOidListSize(false, beanQuery);
				while(iter.hasNext() && ctr<maxSize) {
					X2BaseBean bean = (X2BaseBean) iter.next();
					beansToReturn.add(bean);
					ctr++;
				}
				
				if(iter.hasNext()) {
					// too many beans; let's give up on caching.
					QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), beansToReturn, iter);
					return new BasicEntry<>(dashIter, CacheResults.Type.ABORT_TOO_MANY);
				}
				
				// We have all the beans. Cache the oids for next time and return.

				beanOids = new LinkedList<>();
				for(X2BaseBean bean : beansToReturn) {
					beanOids.add(bean.getOid());
				}
				cache.put(cacheKey, beanOids);
				
				QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), beansToReturn);
				return new BasicEntry<>(dashIter, CacheResults.Type.MISS);
			}
			
			// We're going to try splitting the operator.
			// (That is: if the original operator was "A==true || B==true", then
			// we may split this into "A" or "B" and look those up/cache those results
			// separately.)
			
			Collection<X2BaseBean> knownBeans;
			if(orderBy.getFieldHelpers().isEmpty()) {
				//order doesn't matter
				knownBeans = new LinkedList<>();
			} else {
				knownBeans = new TreeSet<>(orderBy);
			}
			
			Iterator<Operator> splitIter = splitOperators.iterator();
			
			boolean removedOneOrMoreOperators = false;
			while(splitIter.hasNext()) {
				Operator splitOperator = splitIter.next();
				CacheKey splitKey = new CacheKey(splitOperator, orderBy, beanQuery.isDistinct());
				
				List<String> splitOids = cache.get(splitKey);
				if(splitOids!=null) {
					List<X2BaseBean> splitBeans = 
						getBeansFromGlobalCache(broker.getPersistenceKey(), 
							beanQuery.getBaseClass(), splitOids);
					if(splitBeans!=null) {
						// great: we got *some* of the beans by looking at a split query
						removedOneOrMoreOperators = true;
						knownBeans.addAll(splitBeans);
						splitIter.remove();
					} else {
						// We know the exact oids, but those beans aren't in Aspen's cache anymore.
						// This splitOperator is a lost cause now: so ignore it.
						cache.remove(splitKey);
					}
				}
			}
			
			// we removed elements from splitIterators if we resolved those queries, so now
			// all that remains is splitIterators is what we still need to look up.
			
			if(splitOperators.isEmpty()) {
				// we broke the criteria down into small pieces and looked up every piece
				QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), knownBeans);
				return new BasicEntry<>(dashIter, CacheResults.Type.HIT_FROM_SPLIT);
			} else if(removedOneOrMoreOperators) {
				// we resolved *some* operators, but not all of them.
				Operator trimmedOperator = Operator.join(splitOperators.toArray(new Operator[splitOperators.size()]));
				Criteria trimmedCriteria = CriteriaToOperatorConverter.createCriteria(trimmedOperator);
				
				// ... so we're going to make a new (narrower) query, and merge its results with knownBeans
				beanQuery = cloneBeanQuery(beanQuery, trimmedCriteria);
			}
			
			if(knownBeans.isEmpty()) {
				// No cached info came up so far.
				
				// ... so let's just dump incoming beans in a list. The order is going to be correct,
				// because the order is coming straight from the source. So there's no need
				// to use a TreeSet with a comparator anymore:
				
				knownBeans = new LinkedList<>();
			}

			QueryIterator iter = broker.getIteratorByQuery(beanQuery);
			int ctr = 0;
			int maxSize = getMaxOidListSize(removedOneOrMoreOperators, beanQuery);
			while(iter.hasNext() && ctr<maxSize) {
				X2BaseBean bean = (X2BaseBean) iter.next();
				knownBeans.add(bean);
				ctr++;
			}

			if(iter.hasNext()) {
				// too many beans; let's give up on caching.
				QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), knownBeans, iter);
				return new BasicEntry<>(dashIter, CacheResults.Type.ABORT_TOO_MANY);
			}
			
			// we have all the beans; now we just have to stores things in our cache for next time

			beanOids = new LinkedList<>();
			for(X2BaseBean bean : knownBeans) {
				beanOids.add(bean.getOid());
			}
			cache.put(cacheKey, beanOids);
			
			scanOps : for(Operator op : splitOperators) {
				if(isCachingSplitResults(orderBy, profile, op, knownBeans)) {
					List<String> oids = new LinkedList<>();
			
					for(X2BaseBean bean : knownBeans) {
						try {
							if(op.evaluate(BrokerDash.CONTEXT, bean)) {
								oids.add(bean.getOid());
							}
						} catch(Exception e) {
							UncaughtExceptionHandler ueh = getUncaughtExceptionHandler();
							Exception e2 = new Exception("An error occurred evaluating \""+op+"\" on \""+bean+"\"", e);
							ueh.uncaughtException(Thread.currentThread(), e2);
							continue scanOps;
						}
					}

					CacheKey splitKey = new CacheKey(op, orderBy, beanQuery.isDistinct());
					cache.put(splitKey, oids);
				}
			}

			QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), knownBeans);
			if(removedOneOrMoreOperators) {
				return new BasicEntry<>(dashIter, CacheResults.Type.REDUCED_QUERY_FROM_SPLIT);
			} else {
				return new BasicEntry<>(dashIter, CacheResults.Type.MISS);
			}
		}
		
		private QueryByCriteria cloneBeanQuery(QueryByCriteria query,Criteria newCriteria) {
			QueryByCriteria returnValue;
			if(query instanceof BeanQuery) {
				BeanQuery b1 = (BeanQuery) query;
				BeanQuery b2 = b1.copy(true);
				b2.setCriteria(newCriteria);
				returnValue = b2;
			} else {
				returnValue = new QueryByCriteria(query.getBaseClass(), newCriteria);
				for(Object orderBy : query.getOrderBy()) {
					FieldHelper fieldHelper = (FieldHelper) orderBy;
					returnValue.addOrderBy(fieldHelper);
				}
			}
			
			return returnValue;
		}

		/**
		 * Return true if we should iterate through all of the beans and cache
		 * exactly which bean oids are associated with the given operator.
		 */
		protected boolean isCachingSplitResults(OrderByComparator orderBy,
				TemplateQueryProfile profile, Operator op, Collection<X2BaseBean> beansToEvaluate) {
			if(!orderBy.isSimple())
				return false;
			if(beansToEvaluate.size()>100)
				return false;
			return true;
		}

		/**
		 * Clear all cached data from memory.
		 */
		public void clearAll() {
			synchronized(cacheByBeanType) {
				cachePool.clear();
				cacheByBeanType.clear();
			}
		}
		
		public int clearCache(Class beanType) {
			Cache<CacheKey, List<String>> cache = getCache(beanType, false);
			if (cache != null) {
				int size = cache.size();
				cache.clear();
				return size;
			}
			return 0;
		}
		
		/**
		 * Return true if we should consult/update the cache for a given query.
		 */
		protected boolean isCaching(OrderByComparator orderBy, TemplateQueryProfile profile, QueryByCriteria beanQuery) {
			if(profile.getCounter()<10) {
				// The Dash caching layer is supposed to help address
				// frequent repetitive queries. Don't interfere with 
				// rare queries. For large tasks there is usually a huge
				// outermost query/loop (such as grabbing 10,000 students
				// to iterate over). We want to let those big and rare
				// queries slip by this caching model with no interference.
				
				return false;
			}
			
			if(profile.getCounter() > 100 && profile.getAverageReturnCount() > getMaxOidListSize(false, beanQuery)) {
				// If the odds are decent that we're going to get close to
				// our limit: give up now without additional overhead.
				
				return false;
			}
			
			return true;
		}
		
		/**
		 * Return the maximum number of oids we'll cache.
		 */
		protected int getMaxOidListSize(boolean involvedSplit,QueryByCriteria beanQuery) {
			return 500;
		}
		
		/**
		 * Return true if we should split an Operator to evaluate its elements.
		 */
		protected boolean isCachingSplit(OrderByComparator orderBy, TemplateQueryProfile profile, Operator op) {
			if(!orderBy.isSimple()) {
				// When you call "myStudent.getPerson().getAddress()", that may
				// involve two separate database queries. So if our order-by comparator
				// involves fetching these properties: that means we may be issuing
				// lots of queries just to sort beans in the expected order.
				// This defeats the purpose of our caching model: if we saved one
				// query but introduced N-many calls to BeanManager#retrieveReference
				// then we may have just made performance (much) worse.
				
				return false;
			}
			
			return true;
		}

		private ThreadLocal<UncaughtExceptionHandler> uncaughtExceptionHandlers = new ThreadLocal<>();
		static UncaughtExceptionHandler DEFAULT_UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				AppGlobals.getLog().log(Level.SEVERE, "", e);
			}
			
		};
		
		public UncaughtExceptionHandler getUncaughtExceptionHandler() {
			UncaughtExceptionHandler ueh = uncaughtExceptionHandlers.get();
			if(ueh==null)
				ueh = DEFAULT_UNCAUGHT_EXCEPTION_HANDLER;
			return ueh;
		}
		
		public void setUncaughtExceptionHandler(UncaughtExceptionHandler ueh) {
			if(ueh==null) {
				uncaughtExceptionHandlers.remove();
			} else {
				uncaughtExceptionHandlers.set(ueh);
			}
		}
	}
	
	/**
	 * This combines an Operator and a bean Class.
	 * It is used as a key to identify TemplateQueryProfiles.
	 */
	public static class ProfileKey extends BasicEntry<Operator, Class<?>> {
		private static final long serialVersionUID = 1L;

		ProfileKey(Operator operator,Class<?> baseClass) {
			super(operator, baseClass);
		}
	}
	
	/**
	 * This combines an Operator and an OrderByComparator.
	 * It is used as a key to identify a List of oids.
	 */
	public static class CacheKey extends BasicEntry<Operator, OrderByComparator> {
		private static final long serialVersionUID = 1L;
		
		boolean isDistinct;
		
		CacheKey(Operator operator,OrderByComparator orderBy, boolean isDistinct) {
			super(operator, orderBy);
			this.isDistinct = isDistinct;
		}

		@Override
		public boolean equals(Object obj) {
			if(!super.equals(obj))
				return false;
			CacheKey other = (CacheKey) obj;
			if(isDistinct!=other.isDistinct)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "CacheKey[ \""+getKey()+"\", "+getValue()+(isDistinct ? ", distinct" : "")+"]";
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
			PersistenceKey persistenceKey, Class<?> beanClass, String beanOid) {
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
	public static List<X2BaseBean> getBeansFromGlobalCache(PersistenceKey persistenceKey,Class<?> beanType,List<String> beanOids) {
		List<X2BaseBean> beans = new ArrayList<>(beanOids.size());
		for(String beanOid : beanOids) {
			X2BaseBean bean = getBeanFromGlobalCache(persistenceKey, beanType, beanOid);
			if(bean==null)
				return null;
			beans.add(bean);
		}
		return beans;
	}

	public static boolean isBeanQuery(Object object) {
		if(!(object instanceof QueryByCriteria))
			return false;
		QueryByCriteria qbc = (QueryByCriteria) object;
		Class baseClass = qbc.getBaseClass();
		return X2BaseBean.class.isAssignableFrom(baseClass);
	}
	
	@Override
	protected void execute() throws Exception {
		logMessage("Call Dash.convertBroker(broker, parameters) to use this caching model.");
	}

}
