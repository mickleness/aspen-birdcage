package org.abc.dash;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.abc.tools.ThreadedBrokerIterator;
import org.abc.tools.Tool;
import org.abc.util.OrderByComparator;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.ojb.broker.Identity;
import org.apache.ojb.broker.metadata.FieldHelper;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;

import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.X2ObjectCache;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.ModelProperty;
import com.follett.fsc.core.k12.business.PrivilegeSet;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.pump.data.operator.Operator;
import com.pump.data.operator.OperatorContext;
import com.pump.util.Cache;
import com.pump.util.Cache.CachePool;
import com.x2dev.utils.StringUtils;
import com.x2dev.utils.ThreadUtils;

/**
 * The Dash object maintains a cache and a set of shared methods/tools that one
 * or more BrokerDashes can use.
 * <p>
 * In addition to using this class: you'll see additional performance benefits
 * if you multithread your tool using a {@link DashThreadedBrokerIterator}.
 * <p>
 * This class is thread-safe. So you can set up one Dash object and use it
 * across multiple threads. For example: if you set up n-many threads and each
 * thread has a unique X2Broker, then the same Dash object can convert that
 * broker into a caching BrokerDash.
 * <h3>Caching Summary</h3>
 * The Dash caching model relies on two separate caches:
 * <ul><li>A cache of Strings to weak references of beans. The assumption
 * behind this cache is: during the lifetime of a tool you may be keeping
 * strong references to beans around for a long time. Aspen's global cache
 * may drop those beans, but if we've kept a weak reference to them: we
 * can still pull them up (until the weak reference enqueued).</li>
 * <li>A cache of bean queries to their sorted oid results. This is
 * essentially a map where the key is a QueryByCriteria, and the value
 * is a list of oids. (Except it's actually much more complicated than that:
 * there are several conditions under which we'll avoid caching a query,
 * and the queries have to be converted to Operator objects because
 * Query objects do not reliably implementing hashCode/equals.) The
 * assumption behind this cache is: you may reissue the same (or similar)
 * queries several times in a loop inside a tool. This implementation
 * also breaks up ("splits") a query into its OR'ed members and can
 * analyze those members separately. This cache is configured so after
 * X-many elements are added or Y-many milliseconds elapse the oldest 
 * elements are purged from the cache, so it will not grow 
 * indefinitely.</li></ul>
 * <p>
 * <h3>Query Consolidation</h3>
 * This also offers the option to consolidate multiple queries being issued
 * from different threads.
 * <p>
 * Not all queries currently support consolidation. (For example: column queries,
 * or queries with subqueries are not supported.)
 * <p>
 * For example: if one thread queries for "std==1" and another thread queries for "std==2",
 * then if we only allow one active query at a time: one of these threads will query
 * for "std==1 || std==2".
 * <p>
 * This feature only makes sense if you want to use multiple threads. If any thread
 * wants to issue a query and no other thread is blocking it: it will immediately
 * issue its query. (So in a single-thread environment: no competing thread will ever
 * block queries. So for single threads this adds a slight overhead that doesn't
 * offer any benefits.)
 */
@Tool(name = "Dash (Caching Layer)", id = "DASH-CACHE", type = "Procedure")
public class Dash {

	/**
	 * This ThreadedBrokerIterator converts X2Brokers to DashBrokers (so they
	 * include caching), and defers uncaught exceptions to
	 * {@link Dash#getUncaughtExceptionHandler()}.
	 */
	public static class DashThreadedBrokerIterator<Input, Output> extends
			ThreadedBrokerIterator<Input, Output> {

		protected final Dash dash;

		public DashThreadedBrokerIterator(PrivilegeSet privilegeSet,
				BiFunction<X2Broker, Input, Output> function, int threadCount,
				Dash dash, Consumer<Output> outputListener) {
			super(privilegeSet, function, threadCount, outputListener);
			Objects.requireNonNull(dash);
			this.dash = dash;
		}

		@Override
		protected X2Broker createBroker() {
			X2Broker b = super.createBroker();
			b = dash.convertToBrokerDash(b);
			return b;
		}

		@Override
		protected boolean handleUncaughtException(Exception e) {
			dash.getUncaughtExceptionHandler().uncaughtException(
					Thread.currentThread(), e);
			return true;
		}
	}

	/**
	 * This is an OperatorContext for X2BaseBeans. This is the bridge
	 * that connects the Operator architecture with the X2BaseBean
	 * architecture.
	 */
	public static final OperatorContext CONTEXT = new OperatorContext() {

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

		@Override
		public Object getValue(Object dataSource, String attributeName) {
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
	 * This combines an Operator and a bean Class. It is used as a key to
	 * identify TemplateQueryProfiles.
	 */
	public static class TemplateQueryProfileKey extends
			AbstractMap.SimpleEntry<Operator, Class<?>> {
		private static final long serialVersionUID = 1L;

		TemplateQueryProfileKey(Operator operator, Class<?> baseClass) {
			super(operator, baseClass);
		}
	}

	/**
	 * This combines an Operator, an OrderByComparator, and the isDistinct
	 * boolean. It is used as a key to identify a List of oids.
	 */
	public static class CacheKey extends
			AbstractMap.SimpleEntry<Operator, OrderByComparator> {
		private static final long serialVersionUID = 1L;

		boolean isDistinct;

		CacheKey(Operator operator, OrderByComparator orderBy,
				boolean isDistinct) {
			super(operator, orderBy);
			this.isDistinct = isDistinct;
		}

		@Override
		public boolean equals(Object obj) {
			if (!super.equals(obj))
				return false;
			CacheKey other = (CacheKey) obj;
			if (isDistinct != other.isDistinct)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "CacheKey[ \"" + getKey() + "\", " + getValue()
					+ (isDistinct ? ", distinct" : "") + "]";
		}
	}

	/**
	 * Return a bean from the global cache, or return null if the bean does not
	 * exist in the global cache.
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
			beanClass = getBeanTypeFromOid(beanOid);
		}

		Identity identity = new Identity(beanClass, beanClass,
				new Object[] { beanOid });
		X2BaseBean bean = (X2BaseBean) x2cache.lookup(identity);
		return bean;
	}

	/**
	 * Return the bean class associated with an oid.
	 */
	public static Class<?> getBeanTypeFromOid(String beanOid) {
		Objects.requireNonNull(beanOid);
		
		String prefix = beanOid.substring(0, Math.min(3, beanOid.length()));
		BeanTablePath btp = BeanTablePath.getTableByName(prefix.toUpperCase());
		if (btp == null)
			throw new IllegalArgumentException(
					"Could not determine bean class for \"" + beanOid + "\".");
		return btp.getBeanType();
	}

	/**
	 * Return true if the argument is a QueryByCriteria whose base class is an
	 * X2BaseBean.
	 */
	public static boolean isBeanQuery(Object object) {
		if (!(object instanceof QueryByCriteria))
			return false;
		QueryByCriteria qbc = (QueryByCriteria) object;
		Class baseClass = qbc.getBaseClass();
		return X2BaseBean.class.isAssignableFrom(baseClass);
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
			 * This indicates caching wasn't attempted for a bean query.
			 */
			QUERY_SKIP,
			/**
			 * This indicates caching turned up an exact match and no database
			 * query was issued.
			 */
			QUERY_HIT,
			/**
			 * This indicates we gave up on caching because we had too many
			 * beans.
			 */
			QUERY_MISS_ABORT_TOO_MANY,
			/**
			 * This indicates the cache was consulted but didn't have a match.
			 */
			QUERY_MISS,
			/**
			 * This indicates we were able to partially uncache some of the
			 * required beans, and we replaced the original query to identify
			 * the remaining beans.
			 */
			QUERY_REDUCED_FROM_SPLIT,
			/**
			 * This indicates our cache layer was able to resolve the query by
			 * splitting it and resolving its split elements.
			 */
			QUERY_HIT_FROM_SPLIT,
			/**
			 * This indicates caching wasn't attempted because a Criteria
			 * couldn't be converted to an Operator. (This is probably because a
			 * criteria contained a subquery, or some other unsupported
			 * feature).
			 */
			QUERY_SKIP_UNSUPPORTED,
			/**
			 * This indicates a bean was retrieved from a cache of weakly
			 * reference beans. (This is only attempted when the global app's
			 * cache fails.)
			 */
			OID_HIT_REFERENCE,
			/**
			 * This indicates a bean was retrieved from the Aspen's global cache
			 * based on its oid.
			 */
			OID_HIT_ASPEN,
			/**
			 * This indicates we tried to look up a bean by its oid but failed.
			 */
			OID_MISS, 
			/**
			 * This indicates multiple queries were grouped into a query.
			 */
			GROUP_QUERY_ISSUED, 
			/**
			 * This indicates an attempted query was grouped into another query.
			 */
			GROUP_QUERY_HIT;
		}
		
		private static final Comparator<Type> COMPARATOR =
						new Comparator<Type>() {
							@Override
							public int compare(Type o1, Type o2) {
								return o1.name().compareTo(o2.name());
							}
						};

		// sort keys alphabetically so all CacheResults all follow same pattern
		protected Map<Type, AtomicLong> matches = new TreeMap<>(COMPARATOR);

		/**
		 * Increment the counter for a given type of result.
		 * 
		 * @param type
		 *            the type of result to increment.
		 */
		public void increment(Type type) {
			AtomicLong l;
			synchronized (matches) {
				l = matches.get(type);
				if (l == null) {
					l = new AtomicLong(0);
					matches.put(type, l);
				}
			}
			l.incrementAndGet();
			return;
		}

		@Override
		public int hashCode() {
			synchronized (matches) {
				return matches.hashCode();
			}
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof CacheResults))
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
			synchronized (matches) {
				for (Entry<Type, AtomicLong> entry : matches.entrySet()) {
					returnValue.put(entry.getKey(), entry.getValue()
							.longValue());
				}
			}
			return returnValue;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (Entry<Type, AtomicLong> entry : matches.entrySet()) {
				if (sb.length() == 0) {
					sb.append("CacheResults[ " + entry.getKey() + "="
							+ entry.getValue());
				} else {
					sb.append(",\n  " + entry.getKey() + "=" + entry.getValue());
				}
			}
			if (sb.length() == 0)
				sb.append("CacheResults[");
			sb.append("]");
			return sb.toString();
		}

		@SuppressWarnings("unchecked")
		private void readObject(java.io.ObjectInputStream in)
				throws IOException, ClassNotFoundException {
			int version = in.readInt();
			if (version == 0) {
				matches = (Map<Type, AtomicLong>) in.readObject();
			} else {
				throw new IOException("Unsupported internal version: "
						+ version);
			}
		}

		private void writeObject(java.io.ObjectOutputStream out)
				throws IOException {
			out.writeInt(0);
			out.writeObject(matches);
		}
	}

	/**
	 * This monitors a few properties about a template query.
	 * <p>
	 * Here a "template query" means a query stripped of specific values. For
	 * example you query for records where "A==1 || B==true", then the template
	 * of that query resembles "A==? || B==?". Two different queries that use
	 * the same fields but different values match the same template.
	 */
	public static class TemplateQueryProfile implements
			QueryIteratorDash.CloseListener, Serializable {
		private static final long serialVersionUID = 1L;

		protected int ctr = 0;
		protected int maxReturnCount = 0;
		protected double averageReturnCount = 0;
		protected CacheResults results = new CacheResults();

		/**
		 * The total number of times a query matching this template has been
		 * issued.
		 */
		public synchronized int getCounter() {
			return ctr;
		}

		/**
		 * The average number of beans a QueryIterator iterated over.
		 * <p>
		 * In most cases this is synonymous with
		 * "the number of beans a query produces", but if the iterator is
		 * abandoned prematurely then these two values may be different.
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
			averageReturnCount = total / ((double) ctr);
		}

		private void readObject(java.io.ObjectInputStream in)
				throws IOException, ClassNotFoundException {
			int version = in.readInt();
			if (version == 0) {
				ctr = in.readInt();
				maxReturnCount = in.readInt();
				averageReturnCount = in.readDouble();
				results = (CacheResults) in.readObject();
			} else {
				throw new IOException("Unsupported internal version: "
						+ version);
			}
		}

		private void writeObject(java.io.ObjectOutputStream out)
				throws IOException {
			out.writeInt(0);
			out.writeInt(ctr);
			out.writeInt(maxReturnCount);
			out.writeDouble(averageReturnCount);
			out.writeObject(results);
		}

		@Override
		public String toString() {
			return "TemplateQueryProfile[ ctr=" + getCounter()
					+ ", averageReturnCount=" + getAverageReturnCount()
					+ ", maxReturnCount=" + getMaxReturnCount() + ", results="
					+ getResults() + "]";
		}
	}

	public static class QueryRequest {
		public final QueryByCriteria beanQuery;
		public final Operator operator;
		public final TemplateQueryProfile profile;
		public final OrderByComparator orderBy;

		public QueryRequest(QueryByCriteria beanQuery, Operator operator,
				TemplateQueryProfile profile, OrderByComparator orderBy) {
			Objects.requireNonNull(beanQuery);
			Objects.requireNonNull(operator);
			Objects.requireNonNull(profile);
			Objects.requireNonNull(orderBy);

			this.beanQuery = beanQuery;
			this.operator = operator;
			this.profile = profile;
			this.orderBy = orderBy;
		}

		@Override
		public String toString() {
			return "QueryRequest[ query=" + beanQuery + ", operator="
					+ operator + ", profile=" + profile + ", orderBy="
					+ orderBy + "]";
		}
	}

	protected PersistenceKey persistenceKey;
	protected CachePool cachePool;
	protected Cache<TemplateQueryProfileKey, TemplateQueryProfile> profiles;
	protected CacheResults cacheResults = new CacheResults();
	protected Map<Class<?>, Cache<CacheKey, List<String>>> cacheByBeanType = new HashMap<>();

	private Logger log = Logger.getAnonymousLogger();
	private ThreadLocal<Logger> logByThread = new ThreadLocal<>();

	protected Collection<Class> modifiedBeanTypes = new HashSet<>();
	protected UncaughtExceptionHandler uncaughtExceptionHandler = DEFAULT_UNCAUGHT_EXCEPTION_HANDLER;
	protected WeakReferenceBeanCache weakReferenceCache;

	protected boolean isOidCachingActive = true;
	protected boolean isQueryCachingActive = true;

	/**
	 * Create a new Dash that keeps up to 5,0000 elements in the cache for up to
	 * 5 minutes.
	 */
	public Dash(PersistenceKey persistenceKey, int pooledQuerySize) {
		this(persistenceKey, 5000, 1000 * 60 * 5, pooledQuerySize);
	}

	/**
	 * Create a new Dash.
	 * 
	 * @param maxCacheSize
	 *            the maximum number of elements that can exist in the cache.
	 * @param maxCacheDuration
	 *            the maximum duration (in milliseconds) any entry can exist in
	 *            the cache.
	 * @param activeQueryLimit
	 *            the number of threads that can simultaneously issue queries.
	 *            <p>
	 *            If this is not a positive number then this feature is unused.
	 *            See "Query Consolidation" in class javadoc for details.
	 */
	public Dash(PersistenceKey persistenceKey, int maxCacheSize,
			long maxCacheDuration, int activeQueryLimit) {
		this(persistenceKey, new CachePool(maxCacheSize, maxCacheDuration, -1), activeQueryLimit);
	}

	/**
	 * Create a new Dash.
	 * 
	 * @param cachePool
	 *            the CachePool used to maintain all cached data.
	 * @param activeQueryLimit
	 *            the number of threads that can simultaneously issue queries.
	 *            <p>
	 *            If this is not a positive number then this feature is unused.
	 *            See "Query Consolidation" in class javadoc for details.
	 *            
	 */
	public Dash(PersistenceKey persistenceKey, CachePool cachePool, int activeQueryLimit) {
		Objects.requireNonNull(cachePool);
		Objects.requireNonNull(persistenceKey);
		querySemaphore = activeQueryLimit>=1 ? new Semaphore(activeQueryLimit) : null;
		this.cachePool = cachePool;
		this.persistenceKey = persistenceKey;
		profiles = new Cache<>(cachePool);
		getLog().setLevel(Level.OFF);
		weakReferenceCache = new WeakReferenceBeanCache();
		weakReferenceCache.addPropertyListener(new PropertyChangeListener() {

			@Override
			public void propertyChange(PropertyChangeEvent evt) {
				if (WeakReferenceBeanCache.PROPERTY_SIZE.equals(evt
						.getPropertyName())) {
					int oldSize = (Integer) evt.getOldValue();
					int newSize = (Integer) evt.getNewValue();
					int change = newSize - oldSize;
					if (change < 0) {
						if (log != null && log.isLoggable(Level.FINE))
							log.fine("purged " + (-change) + " references");
					}
				}
			}

		});
	}

	/**
	 * Return true if the mechanism that caches WeakReferences of X2BaseBeans by their oids is active.
	 */
	public boolean isOidCachingActive() {
		return isOidCachingActive;
	}

	/**
	 * Return true if the mechanism that caches query results (as a list of oids) by their query is active.
	 */
	public boolean isQueryCachingActive() {
		return isQueryCachingActive;
	}

	public boolean setOidCachingActive(boolean b) {
		if (isOidCachingActive == b)
			return false;
		isOidCachingActive = b;
		return true;
	}

	public boolean setQueryCachingActive(boolean b) {
		if (isQueryCachingActive == b)
			return false;
		isQueryCachingActive = b;
		return true;
	}

	/**
	 * If {@link #isOidCachingActive()} is true then this attempts to return the
	 * requested X2BaseBean without issuing a query.
	 * <p>
	 * First this consults Aspen's default cache. If that fails then this
	 * consults Dash's local WeakReferenceBeanCache.
	 * 
	 * @param beanClass
	 * @param beanOid
	 * @return
	 */
	public X2BaseBean getBeanByOid(Class beanClass, String beanOid) {
		if (isOidCachingActive() == false || beanOid==null)
			return null;

		Logger log = getLog();
		X2BaseBean bean = getBeanFromGlobalCache(getPersistenceKey(), beanClass,
				beanOid);
		if (bean != null) {
			if (log.isLoggable(Level.INFO))
				log.info("global cache resolved " + beanOid);
			cacheResults.increment(CacheResults.Type.OID_HIT_ASPEN);
		} else {
			bean = weakReferenceCache.getBeanByOid(beanClass, beanOid);
			if (bean != null) {
				if (log.isLoggable(Level.INFO))
					log.info("weak references resolved " + beanOid);
				cacheResults.increment(CacheResults.Type.OID_HIT_REFERENCE);
			} else {
				if (log.isLoggable(Level.INFO))
					log.info("no cache resolved " + beanOid);
				cacheResults.increment(CacheResults.Type.OID_MISS);
			}
		}
		return bean;
	}

	/**
	 * Return all the beans in a list of bean oids, or null if any of those
	 * beans were not readily available in the global cache.
	 */
	public List<X2BaseBean> getBeansByOid(
			Class<?> beanType, List<String> beanOids) {
		List<X2BaseBean> beans = new ArrayList<>(beanOids.size());
		for (String beanOid : beanOids) {
			X2BaseBean bean = getBeanByOid(beanType, beanOid);
			if (bean == null)
				return null;
			beans.add(bean);
		}
		return beans;
	}

	/**
	 * Return the Cache associated with a given bean class.
	 * 
	 * @param beanClass
	 *            the type of bean to fetch the cache for.
	 * @param createIfMissing
	 *            if true then may create a new Cache if it doesn't already
	 *            exist. If false then this method may return null.
	 */
	protected Cache<CacheKey, List<String>> getCache(Class<?> beanClass,
			boolean createIfMissing) {
		synchronized (cacheByBeanType) {
			Cache<CacheKey, List<String>> cache = cacheByBeanType
					.get(beanClass);
			if (cache == null && createIfMissing) {
				cache = new Cache<>(cachePool);
				cacheByBeanType.put(beanClass, cache);
			}
			return cache;
		}
	}

	/**
	 * Create a QueryIterator for a QueryByCriteria. If
	 * {@link #isQueryCachingActive()} returns false then this immediately lets
	 * the broker create the default QueryIterator.
	 * <p>
	 * In an ideal case: this will use cached data to completely avoid making a
	 * database query.
	 * <p>
	 * This method should never issue more than one database query. There are 3
	 * database queries this can issue:
	 * <ul>
	 * <li>The original incoming query as-is.</li>
	 * <li>A query to retrieve beans based on oids. If this caching layer was
	 * able to identify the exact oids we need, but those beans are no longer in
	 * Aspen's cache: a query based on the oids should be more efficient.</li>
	 * <li>A query to retrieve a subset of the original query. In this case we
	 * were able to split the original query into smaller pieces, and some of
	 * those pieces we could uncache and others we could not.</li>
	 * </ul>
	 */
	@SuppressWarnings({ "rawtypes" })
	public QueryIterator createQueryIterator(X2Broker broker,
			QueryByCriteria beanQuery) {
		validatePersistenceKey(broker.getPersistenceKey());

		if (!isQueryCachingActive()) {
			QueryIterator iter = broker.getIteratorByQuery(beanQuery);
			QueryIteratorDash dashIter = new QueryIteratorDash(this, null, iter);
			return dashIter;
		}

		Logger log = getLog();
		if (!isBeanQuery(beanQuery)) {
			if (log.isLoggable(Level.INFO))
				log.info("skipping for non-bean-query: " + beanQuery);
			// the DashInvocationHandler won't even call this method if
			// isBeanQuery(..)==false
			cacheResults.increment(CacheResults.Type.QUERY_SKIP_UNSUPPORTED);
			return broker.getIteratorByQuery(beanQuery);
		}

		Operator operator;
		try {
			operator = createOperator(beanQuery.getCriteria());
		} catch (Exception e) {
			// this Criteria can't be converted to an Operator, so we should
			// give up:

			cacheResults.increment(CacheResults.Type.QUERY_SKIP_UNSUPPORTED);
			return broker.getIteratorByQuery(beanQuery);
		}

		Operator template = operator.getTemplateOperator();
		TemplateQueryProfileKey profileKey = new TemplateQueryProfileKey(
				template, beanQuery.getBaseClass());

		if (log.isLoggable(Level.INFO))
			log.info("template: " + template);

		TemplateQueryProfile profile;
		synchronized (profiles) {
			profile = profiles.get(profileKey);
			if (profile == null) {
				profile = new TemplateQueryProfile();
				profiles.put(profileKey, profile);
			}
		}
		if (log.isLoggable(Level.INFO))
			log.info("profile: " + profile);

		OrderByComparator orderBy = new OrderByComparator(false,
				beanQuery.getOrderBy());

		QueryRequest request = new QueryRequest(beanQuery, operator, profile,
				orderBy);

		Map.Entry<QueryIterator, CacheResults.Type> results = createCachedQueryIterator(
				broker, request);
		if (log.isLoggable(Level.INFO))
			log.info("produced " + results);

		QueryIteratorDash dashIter = results.getKey() instanceof QueryIteratorDash ? (QueryIteratorDash) results
				.getKey() : null;
		if (dashIter != null) {
			dashIter.addCloseListener(profile);
			dashIter.addCloseListener(new QueryIteratorDash.CloseListener() {

				@Override
				public void closedIterator(int returnCount, boolean hasNext) {
					Logger log = getLog();
					if (log.isLoggable(Level.INFO))
						log.info("closed iterator after " + returnCount
								+ " iterations, hasNext = " + (hasNext));
				}

			});
		} else {
			// if it's not a QueryIteratorDash then our profile/counting
			// mechanism breaks
			if (log.isLoggable(Level.WARNING))
				log.info("produced a QueryIterator that is not a QueryIteratorDash: "
						+ results.getKey().getClass().getName());
		}
		profile.getResults().increment(results.getValue());
		cacheResults.increment(results.getValue());
		return results.getKey();
	}

	/**
	 * Return the overall cache results of all BeanQueries that passed through
	 * this object.
	 */
	public CacheResults getCacheResults() {
		return cacheResults;
	}

	/**
	 * Create a QueryIterator for the given query. The current implementation of
	 * this method always returns a QueryIteratorDash, but subclasses can
	 * override this to return something else if needed.
	 * 
	 * @return the iterator and the way to classify this request in CacheResults
	 *         objects.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map.Entry<QueryIterator, CacheResults.Type> createCachedQueryIterator(
			X2Broker broker, QueryRequest request) {
		Logger log = getLog();
		if (!isCaching(request)) {
			QueryIterator iter = broker.getIteratorByQuery(request.beanQuery);
			if (log.isLoggable(Level.INFO))
				log.info("aborting to default broker");
			iter = new QueryIteratorDash(this, null, iter);
			return new AbstractMap.SimpleEntry<>(iter,
					CacheResults.Type.QUERY_SKIP);
		}

		boolean usesOids = request.operator.getAttributes().contains(X2BaseBean.COL_OID);

		Cache<CacheKey, List<String>> cache = null;
		CacheKey cacheKey = null;
		List<String> beanOids = null;
		if(!usesOids) {
			cache = getCache(
					request.beanQuery.getBaseClass(), true);
			cacheKey = new CacheKey(request.operator, request.orderBy,
					request.beanQuery.isDistinct());
			beanOids = cache.get(cacheKey);
		}

		if (beanOids != null) {
			List<X2BaseBean> beans = getBeansByOid(request.beanQuery.getBaseClass(), beanOids);
			if (beans != null) {
				// This is our ideal case: we know the complete query results
				QueryIterator dashIter = new QueryIteratorDash(this, beans);
				if (log.isLoggable(Level.INFO))
					log.info("found " + beans.size() + " beans for " + request+": "+beanOids);
				return new AbstractMap.SimpleEntry<>(dashIter,
						CacheResults.Type.QUERY_HIT);
			} else {
				if (log.isLoggable(Level.INFO))
					log.info("found " + beanOids.size() + " beans for " + request+", but they couldn't be uncached: "+beanOids);
			}

			// We know the exact oids, but we couldn't retrieve those beans
			// anymore. In a previous draft we tried rewriting the query based
			// on the oids, but in our field test: this came up about 1 or 2
			// times in two hours. I think it's too much of a risk (we might get
			// something wrong) to worry about this scenario too much
		}

		// we couldn't retrieve the entire query results from our cache

		Operator canonicalOperator = request.operator.getCanonicalOperator();
		Collection<Operator> splitOperators = canonicalOperator.split();

		if (splitOperators.size() <= 1 || !isCachingSplit(request)) {
			// this is the simple scenario (no splitting)
			Collection<X2BaseBean> beansToReturn = new LinkedList<>();
			QueryIterator iter = createPooledQueryIterator(broker, request.beanQuery, request.operator, request.profile, request.orderBy);
			int ctr = 0;
			int maxSize = getMaxOidListSize(false, request.beanQuery);
			while (iter.hasNext() && ctr < maxSize) {
				ThreadUtils.checkInterrupt();

				X2BaseBean bean = (X2BaseBean) iter.next();
				storeBean(bean);
				beansToReturn.add(bean);
				ctr++;
			}

			if (iter.hasNext()) {
				// too many beans; let's give up on caching.
				QueryIterator dashIter = new QueryIteratorDash(this,
						beansToReturn, iter);
				if (log.isLoggable(Level.INFO))
					log.info("query gave up after " + ctr + " iterations for "
							+ request);
				return new AbstractMap.SimpleEntry<>(dashIter,
						CacheResults.Type.QUERY_MISS_ABORT_TOO_MANY);
			}

			// We have all the beans. Cache the oids for next time and return.

			if(cache!=null) {
				beanOids = new ArrayList<>(beansToReturn.size());
				for (X2BaseBean bean : beansToReturn) {
					beanOids.add(bean.getOid());
				}
				cache.put(cacheKey, beanOids);
			}

			QueryIterator dashIter = new QueryIteratorDash(this, beansToReturn);
			if (log.isLoggable(Level.INFO))
				log.info("queried " + ctr + " iterations for " + request+": "+toString(beansToReturn));
			return new AbstractMap.SimpleEntry<>(dashIter,
					CacheResults.Type.QUERY_MISS);
		}

		// We're going to try splitting the operator.
		// (That is: if the original operator was "A==true || B==true", then
		// we may split this into "A" or "B" and look those up/cache those
		// results separately.)

		Collection<X2BaseBean> knownBeans;
		if (request.orderBy.getFieldHelpers().isEmpty()) {
			// order doesn't matter
			knownBeans = new LinkedList<>();
		} else {
			knownBeans = new TreeSet<>(request.orderBy);
		}

		Iterator<Operator> splitOpIter = splitOperators.iterator();

		int removedOperators = 0;
		while (splitOpIter.hasNext()) {
			Operator splitOperator = splitOpIter.next();
			CacheKey splitKey = null;
			List<String> splitOids = null;
			if(cache != null) {
				splitKey = new CacheKey(splitOperator, request.orderBy,
						request.beanQuery.isDistinct());
				splitOids = cache.get(splitKey);
			}

			if (splitOids != null) {
				List<X2BaseBean> splitBeans = getBeansByOid(
						request.beanQuery.getBaseClass(), splitOids);
				if (splitBeans != null) {
					// great: we got *some* of the beans by looking at a split
					// query
					removedOperators++;
					if (log.isLoggable(Level.INFO))
						log.info("resolved split operator " + splitBeans.size()
								+ " beans for \"" + splitOperator+"\": "+splitOids);
					knownBeans.addAll(splitBeans);
					splitOpIter.remove();
				} else {
					// We know the exact oids, but those beans aren't in Aspen's
					// cache anymore.
					// This splitOperator is a lost cause now: so ignore it.
					cache.remove(splitKey);
					if (log.isLoggable(Level.INFO))
						log.info("identified split operator with "
								+ splitOids.size() + " beans, but they couldn't be uncached \""
								+ splitOperator+"\": "+splitOids);
				}
			}
		}

		// we removed elements from splitIterators if we resolved those queries,
		// so now all that remains is splitIterators is what we still need to look 
		// up.

		QueryByCriteria ourQuery = request.beanQuery;
		Operator ourOperator = request.operator;
		if (splitOperators.isEmpty()) {
			// we broke the criteria down into small pieces and looked up every
			// piece
			QueryIterator dashIter = new QueryIteratorDash(this, knownBeans);
			if (log.isLoggable(Level.INFO))
				log.info("collection " + knownBeans.size()
						+ " split beans for " + request);
			return new AbstractMap.SimpleEntry<>(dashIter,
					CacheResults.Type.QUERY_HIT_FROM_SPLIT);
		} else if (removedOperators > 0) {
			// we resolved *some* operators, but not all of them.
			Operator trimmedOperator = Operator.join(splitOperators
					.toArray(new Operator[splitOperators.size()]));
			Criteria trimmedCriteria = createCriteria(trimmedOperator);

			// ... so we're going to make a new (narrower) query, and merge its
			// results with knownBeans
			ourQuery = cloneBeanQuery(request.beanQuery, trimmedCriteria);
			if (log.isLoggable(Level.INFO))
				log.info("removed " + removedOperators + ", rewrote as: "
						+ ourQuery);
			ourOperator = trimmedOperator;
		}

		if (knownBeans.isEmpty()) {
			// No cached info came up so far.

			// ... so let's just dump incoming beans in a list. The order is
			// going to be correct,
			// because the order is coming straight from the source. So there's
			// no need
			// to use a TreeSet with a comparator anymore:

			knownBeans = new LinkedList<>();
		}

		QueryIterator iter = createPooledQueryIterator(broker, ourQuery, ourOperator, request.profile, request.orderBy);
		int ctr = 0;
		int maxSize = getMaxOidListSize(removedOperators > 0, ourQuery);
		while (iter.hasNext() && ctr < maxSize) {
			ThreadUtils.checkInterrupt();

			X2BaseBean bean = (X2BaseBean) iter.next();
			storeBean(bean);
			knownBeans.add(bean);
			ctr++;
		}

		if (iter.hasNext()) {
			// too many beans; let's give up on caching.
			QueryIterator dashIter = new QueryIteratorDash(this, knownBeans,
					iter);
			if (log.isLoggable(Level.INFO))
				log.info("gave up after " + ctr + " iterations for " + request);
			return new AbstractMap.SimpleEntry<>(dashIter,
					CacheResults.Type.QUERY_MISS_ABORT_TOO_MANY);
		}

		// we have all the beans; now we just have to stores things in our cache
		// for next time

		if(cache!=null) {
			beanOids = new ArrayList<>(knownBeans.size());
			for (X2BaseBean bean : knownBeans) {
				beanOids.add(bean.getOid());
			}
			cache.put(cacheKey, beanOids);
		
			if (isCachingSplitResults(request, knownBeans)) {
				scanOps : for (Operator op : splitOperators) {
					List<String> oids = new LinkedList<>();
	
					for (X2BaseBean bean : knownBeans) {
						try {
							if (op.evaluate(Dash.CONTEXT, bean)) {
								oids.add(bean.getOid());
							}
						} catch (Exception e) {
							UncaughtExceptionHandler ueh = getUncaughtExceptionHandler();
							Exception e2 = new Exception(
									"An error occurred evaluating \"" + op
											+ "\" on \"" + bean + "\"", e);
							ueh.uncaughtException(Thread.currentThread(), e2);
							continue scanOps;
						}
					}
	
					CacheKey splitKey = new CacheKey(op, request.orderBy,
							ourQuery.isDistinct());
					cache.put(splitKey, oids);
					if (log.isLoggable(Level.INFO))
						log.info("identified " + oids.size()
								+ " oids for split query \"" + op+"\": "+oids);
				}
			}
		}

		QueryIterator dashIter = new QueryIteratorDash(this, knownBeans);
		if (removedOperators > 0) {
			if (log.isLoggable(Level.INFO))
				log.info("queried " + ctr + " iterations (with "
						+ removedOperators + " cached split queries) for "
						+ request+": "+toString(knownBeans));
			return new AbstractMap.SimpleEntry<>(dashIter,
					CacheResults.Type.QUERY_REDUCED_FROM_SPLIT);
		} else {
			if (log.isLoggable(Level.INFO))
				log.info("queried " + ctr + " iterations for " + request+": "+toString(knownBeans));
			return new AbstractMap.SimpleEntry<>(dashIter,
					CacheResults.Type.QUERY_MISS);
		}
	}
	
	private static String toString(Collection<X2BaseBean> beans) {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		Iterator<X2BaseBean> iter = beans.iterator();
		while(iter.hasNext()) {
			if(sb.length()>1)
				sb.append(", ");
			sb.append(iter.next().getOid());
		}
		sb.append("]");
		return sb.toString();
	}
	
	private static class PooledQueryRequest {
		Operator operator;
		Collection<X2BaseBean> results;
		Semaphore lock = new Semaphore(1);
		
		PooledQueryRequest(Operator operator) {
			this.operator = operator;
		}
		
		@Override
		public String toString() {
			return operator.toString();
		}
	}
	
	Map<Class, Map<CacheKey, Collection<PooledQueryRequest>>> pendingQueries = new HashMap<>();
	
	/**
	 * Queries are issued inside this semaphore (as much as possible).
	 */
	Semaphore querySemaphore;

	protected QueryIterator createPooledQueryIterator(X2Broker broker,QueryByCriteria beanQuery,Operator op,TemplateQueryProfile profile, OrderByComparator comparator) {
		Logger log = getLog();
		boolean abort = false;
		if(querySemaphore==null) {
			abort = true;
		} else if(profile.ctr<100) {
			if(log!=null && log.isLoggable(Level.FINEST)) {
				log.finest("aborting because profile used fewer than 100 times: "+Thread.currentThread().getName()+" "+beanQuery.getBaseClass()+" "+op);
			}
			abort = true;
		} else if(profile.maxReturnCount>500) {
			if(log!=null && log.isLoggable(Level.FINEST)) {
				log.finest("aborting because this profile may return over 500 records: "+Thread.currentThread().getName()+" "+profile+" "+beanQuery.getBaseClass()+" "+op);
			}
			abort = true;
		} else if(!isSimpleAttributes(op.getAttributes())) {
			if(log!=null && log.isLoggable(Level.FINEST)) {
				log.finest("aborting because acceptance criteria is not simple "+Thread.currentThread().getName()+" "+beanQuery.getBaseClass()+" "+op);
			}
			abort = true;
		}
		if(abort) {
			return broker.getIteratorByQuery(beanQuery);
		}
		
		PooledQueryRequest myRequest = new PooledQueryRequest(op);
		Operator template = op.getTemplateOperator();
		CacheKey key = new CacheKey(template, comparator, beanQuery.isDistinct());
		Collection<PooledQueryRequest> similarRequests;
		Map<CacheKey, Collection<PooledQueryRequest>> queryPoolsByKey;
		synchronized(pendingQueries) {
			queryPoolsByKey = pendingQueries.get(beanQuery.getBaseClass());
			if(queryPoolsByKey==null) {
				queryPoolsByKey = new HashMap<>();
				pendingQueries.put(beanQuery.getBaseClass(), queryPoolsByKey);
			}
			similarRequests = queryPoolsByKey.get(key);
			if(similarRequests==null) {
				similarRequests = new LinkedList<>();
				queryPoolsByKey.put(key, similarRequests);
			}
			similarRequests.add(myRequest);
		}
		
		querySemaphore.acquireUninterruptibly();
		try {
			boolean acquiredRequestLocks = false;
			synchronized(pendingQueries) {
				queryPoolsByKey.put(key, new LinkedList<PooledQueryRequest>());
				for(PooledQueryRequest request : similarRequests) {
					if(request.results==null) {
						acquiredRequestLocks = true;
						request.lock.acquireUninterruptibly();
						request.results = new LinkedList<>();
					}
				}
			}
			
			if(acquiredRequestLocks) {
				try {
					if(similarRequests.size()>0) {
						//we are the thread that picked up these requests
						if(similarRequests.size()==1) {
							//nothing clever to do here
							return broker.getIteratorByQuery(beanQuery);
						}
						
						//we have multiple requests in our pool:
						List<Operator> allOperators = new ArrayList<>();
						for(PooledQueryRequest request : similarRequests) {
							for(Operator z : request.operator.split()) {
								allOperators.add(z);
							}
						}
						Operator joined = Operator.join(allOperators);
						Criteria joinedCriteria = createCriteria(joined);
						
						BeanQuery joinedQuery = new BeanQuery(beanQuery.getBaseClass(), joinedCriteria);
						for(FieldHelper h : comparator.getFieldHelpers()) {
							joinedQuery.addOrderBy(h);
						}
						int ctr = 0;
						try(QueryIterator iter = broker.getIteratorByQuery(joinedQuery)) {
							while(iter.hasNext()) {
								X2BaseBean bean = (X2BaseBean) iter.next();
								for(PooledQueryRequest request : similarRequests) {
									if(request.operator.evaluate(CONTEXT, bean)) {
										request.results.add(bean);
									}
								}
								ctr++;
							}
							
							if(log!=null && log.isLoggable(Level.FINER)) {
								log.fine("Pooled "+similarRequests.size()+" into one request, "+ctr+" results: "+Thread.currentThread().getName()+" "+beanQuery.getBaseClass()+" "+joined);
								if(log.isLoggable(Level.FINEST)) {
									for(PooledQueryRequest request : similarRequests) {
										log.finest(request.operator.toString());
									}
								}
							}
							cacheResults.increment(CacheResults.Type.GROUP_QUERY_ISSUED);
						} catch(RuntimeException e) {
							throw e;
						} catch (Exception e) {
							//this shouldn't happen
							throw new RuntimeException(e);
						}
						
						return new QueryIteratorDash(this, myRequest.results);
					}
				} finally {
					for(PooledQueryRequest request : similarRequests) {
						request.lock.release();
					}
				}
			}
		} finally {
			querySemaphore.release();
		}

		//another thread processed our request:
		myRequest.lock.acquireUninterruptibly();
		try {
			if(log!=null && log.isLoggable(Level.FINER)) {
				log.fine("another thread picked up this query: "+Thread.currentThread().getName()+" "+beanQuery.getBaseClass()+" "+op);
			}
			cacheResults.increment(CacheResults.Type.GROUP_QUERY_HIT);
			return new QueryIteratorDash(this, myRequest.results);
		} finally {
			myRequest.lock.release();
		}
	}

	/**
	 * Return true if the argument doesn't contain a period. For example on the
	 * SisStudent bean the attribute "nameView" is simple, but
	 * "person.firstName" is not simple because it relies on a related bean.
	 */
	protected boolean isSimpleAttributes(Collection<String> attributes) {
		for (String attr : attributes) {
			if (attr.indexOf(ModelProperty.PATH_DELIMITER) != -1)
				return false;
		}
		return true;
	}

	/**
	 * Convert an Operator into an Criteria.
	 */
	public Criteria createCriteria(Operator operator) {
		try {
			return getCriteriaToOperatorConverter().createCriteria(operator);
		} finally {
			Logger log = getLog();
			if (log.isLoggable(Level.INFO))
				log.info("" + operator);
		}
	}

	/**
	 * Return the CriteriaToOperatorConverter used to implement
	 * {@link #createCriteria(Operator)} and {@link #createOperator(Criteria)}.
	 */
	protected CriteriaToOperatorConverter getCriteriaToOperatorConverter() {
		return new CriteriaToOperatorConverterImpl();
	}

	/**
	 * Convert a Criteria into an Operator.
	 */
	public Operator createOperator(Criteria criteria) {
		Operator operator = null;
		try {
			operator = getCriteriaToOperatorConverter()
					.createOperator(criteria);
		} finally {
			Logger log = getLog();
			if (log.isLoggable(Level.INFO))
				log.info("" + operator);
		}
		return operator;
	}

	/**
	 * Create a clone of a bean query with new criteria.
	 */
	protected QueryByCriteria cloneBeanQuery(QueryByCriteria query,
			Criteria newCriteria) {
		QueryByCriteria returnValue;
		if (query instanceof BeanQuery) {
			BeanQuery b1 = (BeanQuery) query;
			BeanQuery b2 = b1.copy(true);
			b2.setCriteria(newCriteria);
			returnValue = b2;
		} else {
			returnValue = new QueryByCriteria(query.getBaseClass(), newCriteria);
			for (Object orderBy : query.getOrderBy()) {
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
	protected boolean isCachingSplitResults(QueryRequest request,
			Collection<X2BaseBean> beansToEvaluate) {
		if (!request.orderBy.isSimple())
			return false;
		return true;
	}

	/**
	 * Clear all cached data from memory.
	 * <p>
	 * This is called when {@link X2Broker#clearCache()} is invoked.
	 */
	public void clearAll() {
		try {
			synchronized (cacheByBeanType) {
				cachePool.clear();
				cacheByBeanType.clear();
			}
			weakReferenceCache.clear();
		} finally {
			Logger log = getLog();
			if (log.isLoggable(Level.INFO))
				log.info("");
		}
	}

	/**
	 * This method should be notified when the X2Broker saves/updates/deletes a
	 * particular type of bean.
	 * <p>
	 * This clears our cached records for that bean type, and later if
	 * {@link X2Broker#rollbackTransaction()} is called then it will again clear
	 * our cached records for that bean type. (So if you modify a SisAddress: we
	 * have to clear all our address-related cached info. Then if you rollback
	 * your transaction: we need to clear all our address-related cached info
	 * again.)
	 * 
	 * @param beanType the type of bean that was modified. If this is null
	 * then this method immediately returns.
	 */
	public void modifyBeanRecord(Class beanType) {
		if(beanType==null)
			return;
		
		synchronized (modifiedBeanTypes) {
			modifiedBeanTypes.add(beanType);
		}
		clearCache(beanType);

		Logger log = getLog();
		if (log.isLoggable(Level.INFO))
			log.info(beanType.getName());
	}

	/**
	 * Clear all cached information related to a given bean type.
	 */
	public void clearCache(Class beanType) {
		if(beanType==null)
			return;
		
		int size = 0;
		try {
			Cache<CacheKey, List<String>> cache = getCache(beanType, false);
			if (cache != null) {
				size = cache.size();
				cache.clear();
			}

			size += weakReferenceCache.clear(beanType);
		} finally {
			Logger log = getLog();
			if (log.isLoggable(Level.INFO)) {
				if (size == -1) {
					log.info(beanType + ", no cache available");
				} else {
					log.info(beanType + ", " + size + " entries removed");
				}
			}
		}
	}

	/**
	 * Return true if we should consult/update the cache for a given query.
	 */
	protected boolean isCaching(QueryRequest request) {
		Logger log = getLog();
		if (request.profile.getCounter() < 10) {
			// The Dash caching layer is supposed to help address
			// frequent repetitive queries. Don't interfere with
			// rare queries. For large tasks there is usually a huge
			// outermost query/loop (such as grabbing 10,000 students
			// to iterate over). We want to let those big and rare
			// queries slip by this caching model with no interference.

			if (log.isLoggable(Level.INFO))
				log.info("skipping because profile is too small: "
						+ request.profile);

			return false;
		}

		int max = getMaxOidListSize(false, request.beanQuery);
		if (request.profile.getCounter() > 100
				&& request.profile.getAverageReturnCount() > max) {
			// If the odds are decent that we're going to get close to
			// our limit: give up now without additional overhead.

			if (log.isLoggable(Level.INFO))
				log.info("skipping because profile shows average exceeds "
						+ max + ": " + request.profile);

			return false;
		}

		return true;
	}

	/**
	 * Return the maximum number of oids we'll cache.
	 */
	protected int getMaxOidListSize(boolean involvedSplit, QueryByCriteria query) {
		return 500;
	}

	/**
	 * Return true if we should split an Operator to evaluate its elements.
	 */
	protected boolean isCachingSplit(QueryRequest request) {
		if (!request.orderBy.isSimple()) {
			// When you call "myStudent.getPerson().getAddress()", that may
			// involve two separate database queries. So if our order-by
			// comparator
			// involves fetching these properties: that means we may be issuing
			// lots of queries just to sort beans in the expected order.
			// This defeats the purpose of our caching model: if we saved one
			// query but introduced N-many calls to
			// BeanManager#retrieveReference
			// then we may have just made performance (much) worse.

			return false;
		}

		return true;
	}

	static UncaughtExceptionHandler DEFAULT_UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			AppGlobals.getLog().log(Level.SEVERE, "", e);
		}

	};

	/**
	 * This UncaughtExceptionHandler does nothing; it should be used if you call
	 * {@link #setUncaughtExceptionHandler(UncaughtExceptionHandler)} and pass
	 * in null.
	 */
	static UncaughtExceptionHandler NULL_UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			// intentionally empty
		}

	};

	/**
	 * Return the UncaughtExceptionHandler. The default handler writes the stack
	 * trace to the AppGlobals log.
	 */
	public UncaughtExceptionHandler getUncaughtExceptionHandler() {
		return uncaughtExceptionHandler;
	}

	/**
	 * Assign the UncaughtExceptionHandler.
	 * 
	 * @param ueh
	 *            the new UncaughtExceptionHandler. If this is null then an
	 *            empty UncaughtExceptionHandler is used (that does nothing).
	 */
	public void setUncaughtExceptionHandler(UncaughtExceptionHandler ueh) {
		if (ueh == null)
			ueh = NULL_UNCAUGHT_EXCEPTION_HANDLER;
		uncaughtExceptionHandler = ueh;
	}

	/**
	 * Create a BrokerDash that uses this factory's CachePool.
	 * <p>
	 * It is safe to call this method redundantly. If the argument already uses
	 * this object's cache then the argument broker is returned as-is.
	 * <p>
	 */
	public BrokerDash convertToBrokerDash(X2Broker broker) {
		validatePersistenceKey(broker.getPersistenceKey());

		if (broker instanceof BrokerDash) {
			BrokerDash bd = (BrokerDash) broker;
			Dash sharedResource = bd.getDash();
			if (sharedResource == this) {
				return bd;
			}
		}
		BrokerDash returnValue = createBrokerDash(broker);
		return returnValue;
	}

	private void validatePersistenceKey(PersistenceKey otherPersistenceKey) {
		if (persistenceKey != otherPersistenceKey)
			throw new IllegalStateException(persistenceKey + " != "
					+ otherPersistenceKey);
	}

	@SuppressWarnings("rawtypes")
	private BrokerDash createBrokerDash(X2Broker broker) {
		InvocationHandler handler = new DashInvocationHandler(broker, this);

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

	/**
	 * Return the CachePool used by all caches this Dash object maintains.
	 */
	public CachePool getCachePool() {
		return cachePool;
	}

	/**
	 * Return a Writer to log debugging information to.
	 */
	public Logger getLog() {
		Logger r = logByThread.get();
		if (r == null)
			r = log;
		return r;
	}

	/**
	 * Set a log for debugging.
	 * 
	 * @param appendable
	 *            the new Appendable to append output to
	 * @param threadSpecific
	 *            if true then this Appendable will only be consulted for the
	 *            current thread. If false then this Appendable will apply to
	 *            all threads.
	 */
	public void setLog(Logger log, boolean threadSpecific) {
		if (threadSpecific) {
			logByThread.set(log);
		} else {
			logByThread.set(null);
			this.log = log;
		}
	}

	/**
	 * This is called during {@link X2Broker#rollbackTransaction()} to clear all
	 * cached information related beans that may have been changed during this
	 * rollback.
	 */
	public void clearModifiedBeanTypes() {
		Class[] z;
		synchronized (modifiedBeanTypes) {
			z = modifiedBeanTypes.toArray(new Class[modifiedBeanTypes.size()]);
			modifiedBeanTypes.clear();
		}

		for (Class c : z) {
			clearCache(c);
		}

		Logger log = getLog();
		if (log.isLoggable(Level.INFO))
			log.info(Arrays.asList(z).toString());
	}

	public PersistenceKey getPersistenceKey() {
		return persistenceKey;
	}

	protected void storeBean(X2BaseBean bean) {
		if (isOidCachingActive()) {
			weakReferenceCache.storeBean(bean);
		}
	}

	/**
	 * This listener will be notified when the
	 * {@link WeakReferenceBeanCache#PROPERTY_SIZE} property changes.
	 */
	public void addWeakReferencePropertyListener(PropertyChangeListener l) {
		weakReferenceCache.addPropertyListener(l);
	}

	public void removeWeakReferencePropertyListener(PropertyChangeListener l) {
		weakReferenceCache.addPropertyListener(l);
	}
}
