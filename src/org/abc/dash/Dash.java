package org.abc.dash;

import java.io.IOException;
import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.abc.tools.Tool;
import org.abc.util.BasicEntry;
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
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.pump.data.operator.And;
import com.pump.data.operator.EqualTo;
import com.pump.data.operator.Operator;
import com.pump.data.operator.OperatorContext;
import com.pump.util.Cache;
import com.pump.util.Cache.CachePool;
import com.x2dev.utils.StringUtils;

/**
 * The Dash object maintains a cache and a set of shared methods/tools
 * that one or more BrokerDashes can use.
 * <p>
 * This class is thread-safe. So you can set up one Dash object and use it
 * across multiple threads. For example: if you set up n-many threads and 
 * each thread has a unique X2Broker, then the same Dash object can convert
 * that broker into a caching BrokerDash.
 */
@Tool(name = "Dash Caching Model", id = "DASH-CACHE", type="Procedure")
public class Dash {

	/**
	 * This is an OperatorContext that connects Operators to X2BaseBeans.
	 */
	public static final OperatorContext CONTEXT = new OperatorContext() {

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
	 * This combines an Operator, an OrderByComparator, and the isDistinct boolean.
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
			beanClass = getBeanTypeFromOid(beanOid);
		}

		Identity identity = new Identity(beanClass, beanClass,
				new Object[] { beanOid });
		X2BaseBean bean = (X2BaseBean) x2cache.lookup(identity);
		return bean;
	}

	public static Class<?> getBeanTypeFromOid(String beanOid) {
		String prefix = beanOid.substring(0, Math.min(3, beanOid.length()));
		BeanTablePath btp = BeanTablePath.getTableByName(prefix
				.toUpperCase());
		if (btp == null)
			throw new IllegalArgumentException(
					"Could not determine bean class for \"" + beanOid
							+ "\".");
		return btp.getBeanType();
	}

	/**
	 * Return all the beans in a list of bean oids, or null if any of those beans were not readily available in the global cache.
	 */
	public List<X2BaseBean> getBeans(PersistenceKey persistenceKey,Class<?> beanType,List<String> beanOids) {
		List<X2BaseBean> beans = new ArrayList<>(beanOids.size());
		for(String beanOid : beanOids) {
			X2BaseBean bean = getBeanByOid(persistenceKey, beanType, beanOid);
			if(bean==null)
				return null;
			beans.add(bean);
		}
		return beans;
	}

	/**
	 * Return true if the argument is a QueryByCriteria whose base class is an X2BaseBean.
	 */
	public static boolean isBeanQuery(Object object) {
		if(!(object instanceof QueryByCriteria))
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
			SKIP_UNSUPPORTED,
			/**
			 * This unusual case indicates that an oid was directly embedded in the query (possibly with other query criteria)
			 */
			HIT_FROM_OID,
			/**
			 * This indicates a bean was retrieved from a cache of weakly reference beans. (This is only
			 * attempted when the global app's cache fails.)
			 */
			OID_HIT_REFERENCE,
			/**
			 * This indicates a bean was retrieved from the Aspen's global cache based on its oid.
			 */
			OID_HIT_ASPEN,
			/**
			 * This indicates we tried to look up a bean by its oid but failed.
			 */
			OID_MISS;
		}
		
		//sort keys alphabetically so all CacheResults follow same pattern
		protected Map<Type, AtomicLong> matches = new TreeMap<>();

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
			StringBuilder sb = new StringBuilder();
			for(Entry<Type, AtomicLong> entry : matches.entrySet()) {
				if(sb.length()==0) {
					sb.append("CacheResults[ "+entry.getKey()+"="+entry.getValue());
				} else {
					sb.append(",\n  "+entry.getKey()+"="+entry.getValue());
				}
			}
			if(sb.length()==0)
				sb.append("CacheResults[");
			sb.append("]");
			return sb.toString();
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
	 * This monitors a few properties about a template query.
	 * <p>
	 * Here a "template query" means a query stripped of specific values. For example
	 * you query for records where "A==1 || B==true", then the template of that query
	 * resembles "A==? || B==?". Two different queries that use the same fields but
	 * different values match the same template.
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
			return "QueryRequest[ query="+beanQuery+", operator="+operator+", profile="+profile+", orderBy="+orderBy+"]";
		}
		
	}
	
	protected PersistenceKey persistenceKey;
	protected CachePool cachePool;
	protected Cache<ProfileKey, TemplateQueryProfile> profiles;
	protected CacheResults cacheResults = new CacheResults();
	protected Map<Class<?>, Cache<CacheKey, List<String>>> cacheByBeanType = new HashMap<>();
	
	private Logger log = Logger.getAnonymousLogger();
	private ThreadLocal<Logger> logByThread = new ThreadLocal<>();
	
	protected Collection<Class> modifiedBeanTypes = new HashSet<>();
	protected UncaughtExceptionHandler uncaughtExceptionHandler = DEFAULT_UNCAUGHT_EXCEPTION_HANDLER;
	protected WeakReferenceBeanCache weakReferenceCache;
	
	/**
	 * Create a new Dash that keeps up to 5,0000 elements in the cache for up to 5 minutes.
	 */
	public Dash(PersistenceKey persistenceKey) {
		this(persistenceKey, 5000, 1000*60*5);
	}
	
	/**
	 * Create a new Dash.
	 * 
	 * @param maxCacheSize the maximum number of elements that can exist in the cache.
	 * @param maxCacheDuration the maximum duration (in milliseconds) any entry
	 * can exist in the cache.
	 */
	public Dash(PersistenceKey persistenceKey,int maxCacheSize, long maxCacheDuration) {
		this(persistenceKey, new CachePool(maxCacheSize, maxCacheDuration, -1));
	}
	
	/**
	 * Create a new Dash.
	 * 
	 * @param cachePool the CachePool used to maintain all cached data.
	 */
	public Dash(PersistenceKey persistenceKey,CachePool cachePool) {
		Objects.requireNonNull(cachePool);
		Objects.requireNonNull(persistenceKey);
		this.cachePool = cachePool;
		this.persistenceKey = persistenceKey;
		profiles = new Cache<>(cachePool);
		getLog().setLevel(Level.OFF);
		weakReferenceCache = new WeakReferenceBeanCache(this);
	}
	
	
	public X2BaseBean getBeanByOid(PersistenceKey persistenceKey, Class beanClass,String beanOid) {
		Logger log = getLog();
		X2BaseBean bean = getBeanFromGlobalCache(persistenceKey, beanClass, beanOid);
		if(bean!=null) {
			if(log.isLoggable(Level.INFO))
				log.info("global cache resolved "+beanOid);
			cacheResults.increment(CacheResults.Type.OID_HIT_ASPEN);
		} else {
			bean = weakReferenceCache.getBeanByOid(beanClass, beanOid);
			if(bean!=null) {
				if(log.isLoggable(Level.INFO))
					log.info("weak references resolved "+beanOid);
				cacheResults.increment(CacheResults.Type.OID_HIT_REFERENCE);
			} else {
				if(log.isLoggable(Level.INFO))
					log.info("no cache resolved "+beanOid);
				cacheResults.increment(CacheResults.Type.OID_MISS);
			}
		}
		return bean;
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
	 * Create a QueryIterator for a QueryByCriteria.
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
	@SuppressWarnings({ "rawtypes" })
	public QueryIterator createQueryIterator(X2Broker broker,QueryByCriteria beanQuery) {
		validatePersistenceKey(broker.getPersistenceKey());
		
		Logger log = getLog();
		if(!isBeanQuery(beanQuery)) {
			if(log.isLoggable(Level.INFO))
				log.info("skipping for non-bean-query: "+beanQuery);
			// the DashInvocationHandler won't even call this method if isBeanQuery(..)==false
			cacheResults.increment(CacheResults.Type.SKIP_UNSUPPORTED);
			return broker.getIteratorByQuery(beanQuery);
		}

		Operator operator;
		try {
			operator = createOperator(beanQuery.getCriteria());
		} catch(Exception e) {
			//this Criteria can't be converted to an Operator, so we should give up:

			cacheResults.increment(CacheResults.Type.SKIP_UNSUPPORTED);
			return broker.getIteratorByQuery(beanQuery);
		}
		
		Operator template = operator.getTemplateOperator();
		ProfileKey profileKey = new ProfileKey(template, beanQuery.getBaseClass());

		if(log.isLoggable(Level.INFO))
			log.info("template: "+template);
		
		TemplateQueryProfile profile;
		synchronized(profiles) {
			profile = profiles.get(profileKey);
			if(profile==null) {
				profile = new TemplateQueryProfile();
				profiles.put(profileKey, profile);
			}
		}
		if(log.isLoggable(Level.INFO))
			log.info("profile: "+profile);
		
		OrderByComparator orderBy = new OrderByComparator(false,
				beanQuery.getOrderBy());
		
		QueryRequest request = new QueryRequest(beanQuery, operator, profile, orderBy);
		
		Map.Entry<QueryIterator,CacheResults.Type> results = doCreateQueryIterator(broker, request);
		if(log.isLoggable(Level.INFO))
			log.info("produced "+results);
		
		QueryIteratorDash dashIter = results.getKey() instanceof QueryIteratorDash ? (QueryIteratorDash) results.getKey() : null;
		if(dashIter!=null) {
			dashIter.addCloseListener(profile);
			dashIter.addCloseListener(new QueryIteratorDash.CloseListener() {

				@Override
				public void closedIterator(int returnCount, boolean hasNext) {
					Logger log = getLog();
					if(log.isLoggable(Level.INFO))
						log.info("closed iterator after "+returnCount+" iterations, hasNext = "+(hasNext));
				}
				
			});
		} else {
			// if it's not a QueryIteratorDash then our profile/counting mechanism breaks
			if(log.isLoggable(Level.WARNING))
				log.info("produced a QueryIterator that is not a QueryIteratorDash: "+results.getKey().getClass().getName());
		}
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
	 * Create a QueryIterator for the given query. The current implementation of this
	 * method always returns a QueryIteratorDash, but subclasses can override this
	 * to return something else if needed.
	 * 
	 * @return the iterator and the way to classify this request in CacheResults objects.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map.Entry<QueryIterator,CacheResults.Type> doCreateQueryIterator(X2Broker broker, QueryRequest request) {
		Logger log = getLog();
		if(!isCaching(request)) {
			QueryIterator iter = broker.getIteratorByQuery(request.beanQuery);
			if(log.isLoggable(Level.INFO))
				log.info("aborting to default broker");
			iter = new QueryIteratorDash(this, null, iter);
			return new BasicEntry<>(iter, CacheResults.Type.SKIP);
		}
		
		Cache<CacheKey, List<String>> cache = getCache(request.beanQuery.getBaseClass(), true);
		CacheKey cacheKey = new CacheKey(request.operator, request.orderBy, request.beanQuery.isDistinct());
		List<String> beanOids = cache.get(cacheKey);
		
		if (beanOids != null) {
			List<X2BaseBean> beans = getBeans(broker.getPersistenceKey(), request.beanQuery.getBaseClass(), beanOids);
			if(beans!=null) {
				// This is our ideal case: we know the complete query results
				QueryIterator dashIter = new QueryIteratorDash(this, beans);
				if(log.isLoggable(Level.INFO))
					log.info("found "+beans.size()+" beans for "+request);
				return new BasicEntry<>(dashIter, CacheResults.Type.HIT);
			}
			
			// We know the exact oids, but those beans aren't in Aspen's cache 
			// anymore. We can at least rewrite the query:
			
			Criteria oidCriteria = new Criteria();
			oidCriteria.addIn(X2BaseBean.COL_OID, beanOids);
			QueryByCriteria newQuery = cloneBeanQuery(request.beanQuery, oidCriteria);

			QueryIterator iter = broker.getIteratorByQuery(newQuery);
			QueryIterator dashIter = new QueryIteratorDash(this, null, iter);

			if(log.isLoggable(Level.INFO))
				log.info("found "+beanOids.size()+" bean oids for "+request);
			
			return new BasicEntry<>(dashIter, CacheResults.Type.REDUCED_QUERY_TO_OIDS);
		}
		
		// we couldn't retrieve the entire query results from our cache

		Operator canonicalOperator = request.operator.getCanonicalOperator();
		Collection<Operator> splitOperators = canonicalOperator.split();
		
		if(isSimpleAttributes(canonicalOperator.getAttributes())) {
			
			Collection<X2BaseBean> beansFromOperator = getBeansFromSplitOperator(broker.getPersistenceKey(), request.beanQuery.getBaseClass(), splitOperators);
			givenSpecificOids : if(beansFromOperator!=null) {
				
				//this is an odd case, but it came up in real-world tests:
				//the query specifically gives us the oid, possibly with other conditions.
				//such as:
				//  contains(oid, {"GRQ00000063MDb", "GRQ00000063M5p"}) && programStudiesOid == "GPR0000001e0Cp"
				//or:
				//  oid == "std01000055744" && studentEvents.eventType == "GRADUATION REQUIREMENT"
				
				// the former should be simple enough to convert using getBeanByOids and evaluate.
				// the latter (because it relies on a related bean) is not safe to optimize here.
				
				Collection<X2BaseBean> returnValue = new TreeSet<>(request.orderBy);
				for(X2BaseBean bean : beansFromOperator) {
					try {
						if(request.operator.evaluate(Dash.CONTEXT, beansFromOperator))
							returnValue.add(bean);
					} catch(Exception e) {
						getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
						break givenSpecificOids;
					}
				}
				QueryIterator dashIter = new QueryIteratorDash(this, returnValue);
				if(log.isLoggable(Level.INFO))
					log.info("filtered "+returnValue.size()+" bean oids for "+request);
				return new BasicEntry<>(dashIter, CacheResults.Type.HIT_FROM_OID);
			}
		}

		if(splitOperators.size() <= 1 || !isCachingSplit(request)) {
			// this is the simple scenario (no splitting)
			Collection<X2BaseBean> beansToReturn = new LinkedList<>();
			QueryIterator iter = broker.getIteratorByQuery(request.beanQuery);
			int ctr = 0;
			int maxSize = getMaxOidListSize(false, request.beanQuery);
			while(iter.hasNext() && ctr<maxSize) {
				X2BaseBean bean = (X2BaseBean) iter.next();
				weakReferenceCache.storeBean(bean);
				beansToReturn.add(bean);
				ctr++;
			}
			
			if(iter.hasNext()) {
				// too many beans; let's give up on caching.
				QueryIterator dashIter = new QueryIteratorDash(this, beansToReturn, iter);
				if(log.isLoggable(Level.INFO))
					log.info("query gave up after "+ctr+" iterations for "+request);
				return new BasicEntry<>(dashIter, CacheResults.Type.ABORT_TOO_MANY);
			}
			
			// We have all the beans. Cache the oids for next time and return.

			beanOids = new LinkedList<>();
			for(X2BaseBean bean : beansToReturn) {
				beanOids.add(bean.getOid());
			}
			cache.put(cacheKey, beanOids);
			
			QueryIterator dashIter = new QueryIteratorDash(this, beansToReturn);
			if(log.isLoggable(Level.INFO))
				log.info("queried "+ctr+" iterations for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.MISS);
		}
		
		// We're going to try splitting the operator.
		// (That is: if the original operator was "A==true || B==true", then
		// we may split this into "A" or "B" and look those up/cache those results
		// separately.)
		
		Collection<X2BaseBean> knownBeans;
		if(request.orderBy.getFieldHelpers().isEmpty()) {
			//order doesn't matter
			knownBeans = new LinkedList<>();
		} else {
			knownBeans = new TreeSet<>(request.orderBy);
		}
		
		Iterator<Operator> splitIter = splitOperators.iterator();
		
		int removedOperators = 0;
		while(splitIter.hasNext()) {
			Operator splitOperator = splitIter.next();
			CacheKey splitKey = new CacheKey(splitOperator, request.orderBy, request.beanQuery.isDistinct());
			
			List<String> splitOids = cache.get(splitKey);
			if(splitOids!=null) {
				List<X2BaseBean> splitBeans = 
					getBeans(broker.getPersistenceKey(), 
							request.beanQuery.getBaseClass(), splitOids);
				if(splitBeans!=null) {
					// great: we got *some* of the beans by looking at a split query
					removedOperators++;
					if(log.isLoggable(Level.INFO))
						log.info("resolved split operator "+splitBeans.size()+" beans: "+splitOperator);
					knownBeans.addAll(splitBeans);
					splitIter.remove();
				} else {
					// We know the exact oids, but those beans aren't in Aspen's cache anymore.
					// This splitOperator is a lost cause now: so ignore it.
					cache.remove(splitKey);
					if(log.isLoggable(Level.INFO))
						log.info("identified split operator with "+splitOids.size()+" beans, but purged it: "+splitOperator);
				}
			}
		}
		
		// we removed elements from splitIterators if we resolved those queries, so now
		// all that remains is splitIterators is what we still need to look up.
		
		QueryByCriteria ourQuery = request.beanQuery;
		if(splitOperators.isEmpty()) {
			// we broke the criteria down into small pieces and looked up every piece
			QueryIterator dashIter = new QueryIteratorDash(this, knownBeans);
			if(log.isLoggable(Level.INFO))
				log.info("collection "+knownBeans.size()+" split beans for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.HIT_FROM_SPLIT);
		} else if(removedOperators>0) {
			// we resolved *some* operators, but not all of them.
			Operator trimmedOperator = Operator.join(splitOperators.toArray(new Operator[splitOperators.size()]));
			Criteria trimmedCriteria = createCriteria(trimmedOperator);
			
			// ... so we're going to make a new (narrower) query, and merge its results with knownBeans
			ourQuery = cloneBeanQuery(request.beanQuery, trimmedCriteria);
			if(log.isLoggable(Level.INFO))
				log.info("removed "+ removedOperators+", rewrote as: "+ourQuery);
		}
		
		if(knownBeans.isEmpty()) {
			// No cached info came up so far.
			
			// ... so let's just dump incoming beans in a list. The order is going to be correct,
			// because the order is coming straight from the source. So there's no need
			// to use a TreeSet with a comparator anymore:
			
			knownBeans = new LinkedList<>();
		}

		QueryIterator iter = broker.getIteratorByQuery(ourQuery);
		int ctr = 0;
		int maxSize = getMaxOidListSize(removedOperators>0, ourQuery);
		while(iter.hasNext() && ctr<maxSize) {
			X2BaseBean bean = (X2BaseBean) iter.next();
			weakReferenceCache.storeBean(bean);
			knownBeans.add(bean);
			ctr++;
		}

		if(iter.hasNext()) {
			// too many beans; let's give up on caching.
			QueryIterator dashIter = new QueryIteratorDash(this, knownBeans, iter);
			if(log.isLoggable(Level.INFO))
				log.info("gave up after "+ctr+" iterations for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.ABORT_TOO_MANY);
		}
		
		// we have all the beans; now we just have to stores things in our cache for next time

		beanOids = new LinkedList<>();
		for(X2BaseBean bean : knownBeans) {
			beanOids.add(bean.getOid());
		}
		cache.put(cacheKey, beanOids);
		
		scanOps : for(Operator op : splitOperators) {
			if(isCachingSplitResults(request, knownBeans)) {
				List<String> oids = new LinkedList<>();
		
				for(X2BaseBean bean : knownBeans) {
					try {
						if(op.evaluate(Dash.CONTEXT, bean)) {
							oids.add(bean.getOid());
						}
					} catch(Exception e) {
						UncaughtExceptionHandler ueh = getUncaughtExceptionHandler();
						Exception e2 = new Exception("An error occurred evaluating \""+op+"\" on \""+bean+"\"", e);
						ueh.uncaughtException(Thread.currentThread(), e2);
						continue scanOps;
					}
				}

				CacheKey splitKey = new CacheKey(op, request.orderBy, ourQuery.isDistinct());
				cache.put(splitKey, oids);
				if(log.isLoggable(Level.INFO))
					log.info("identified "+oids.size()+" oids for split query "+op);
			}
		}

		QueryIterator dashIter = new QueryIteratorDash(this, knownBeans);
		if(removedOperators > 0) {
			if(log.isLoggable(Level.INFO))
				log.info("queried "+ctr+" iterations (with "+removedOperators+" cached split queries) for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.REDUCED_QUERY_FROM_SPLIT);
		} else {
			if(log.isLoggable(Level.INFO))
				log.info("queried "+ctr+" iterations for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.MISS);
		}
	}
	
	protected Collection<X2BaseBean> getBeansFromSplitOperator(PersistenceKey persistenceKey, Class beanClass, Collection<Operator> splitOperators) {
		Collection<X2BaseBean> beans = new HashSet<>();
		Logger log = getLog();
		int ctr = 0;
		for(Operator op : splitOperators) {
			
			List ops;
			if(op instanceof And) {
				ops = ((And)op).getOperands();
			} else {
				ops = new ArrayList<>();
				ops.add(op);
			}
			
			EqualTo oidEqualTo = null;
			for(Object z : ops) {
				if(z instanceof EqualTo && X2BaseBean.COL_OID.equals( ((EqualTo)z).getAttribute()) ) {
					oidEqualTo = (EqualTo) z;
					break;
				}
			}
			X2BaseBean bean = oidEqualTo==null ? null :
				getBeanByOid(persistenceKey, beanClass, oidEqualTo.getAttribute() );
			
			if(bean==null) {
				if(log.isLoggable(Level.INFO))
					log.info("aborting after "+ctr+" beans");
				return null;
			}
			
			beans.add(bean);
			ctr++;
		}
		
		if(log.isLoggable(Level.INFO))
			log.info("returning "+beans.size()+" beans");
		
		return beans;
	}

	/**
	 * Return true if the argument doesn't contain a period. For example on the SisStudent bean 
	 * the attribute "nameView" is simple, but "person.firstName" is not simple because it relies 
	 * on a related bean.
	 */
	protected boolean isSimpleAttributes(Collection<String> attributes) {
		for(String attr : attributes) {
			if(attr.indexOf(ModelProperty.PATH_DELIMITER)!=-1)
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
			if(log.isLoggable(Level.INFO))
				log.info(""+operator);
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
			operator = getCriteriaToOperatorConverter().createOperator(criteria);
		} finally {
			Logger log = getLog();
			if(log.isLoggable(Level.INFO))
				log.info(""+operator);
		}
		return operator;
	}

	/** 
	 * Create a clone of a bean query with new criteria.
	 */
	protected QueryByCriteria cloneBeanQuery(QueryByCriteria query,Criteria newCriteria) {
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
	protected boolean isCachingSplitResults(QueryRequest request, Collection<X2BaseBean> beansToEvaluate) {
		if(!request.orderBy.isSimple())
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
			synchronized(cacheByBeanType) {
				cachePool.clear();
				cacheByBeanType.clear();
			}
			weakReferenceCache.clear();
		} finally {
			Logger log = getLog();
			if(log.isLoggable(Level.INFO))
				log.info("");
		}
	}
	
	/**
	 * This method should be notified when the X2Broker saves/updates/deletes
	 * a particular type of bean.
	 * <p>
	 * This clears our cached records for that bean type, and later if
	 * {@link X2Broker#rollbackTransaction()} is called then it will
	 * again clear our cached records for that bean type. (So if you
	 * modify a SisAddress: we have to clear all our address-related cached info.
	 * Then if you rollback your transaction: we need to clear all our
	 * address-related cached info again.)
	 */
	public void modifyBeanRecord(Class beanType) {
		Objects.requireNonNull(beanType);
		synchronized(modifiedBeanTypes) {
			modifiedBeanTypes.add(beanType);
		}
		clearCache(beanType);
	}
	
	public int clearCache(Class beanType) {
		Objects.requireNonNull(beanType);
		int size = -1;
		try {
			Cache<CacheKey, List<String>> cache = getCache(beanType, false);
			if (cache != null) {
				size = cache.size();
				cache.clear();
				return size;
			}
			
			weakReferenceCache.clear(beanType);
			return 0;
		} finally {
			Logger log = getLog();
			if(log.isLoggable(Level.INFO)) {
				if(size==-1) {
					log.info(beanType+", no cache available");
				} else {
					log.info(beanType+", "+size+" entries removed");
				}
			}
		}
	}
	
	/**
	 * Return true if we should consult/update the cache for a given query.
	 */
	protected boolean isCaching(QueryRequest request) {
		Logger log = getLog();
		if(request.profile.getCounter()<10) {
			// The Dash caching layer is supposed to help address
			// frequent repetitive queries. Don't interfere with 
			// rare queries. For large tasks there is usually a huge
			// outermost query/loop (such as grabbing 10,000 students
			// to iterate over). We want to let those big and rare
			// queries slip by this caching model with no interference.

			if(log.isLoggable(Level.INFO))
				log.info("skipping because profile is too small: "+request.profile);
			
			return false;
		}
		
		int max = getMaxOidListSize(false, request.beanQuery);
		if(request.profile.getCounter() > 100 && request.profile.getAverageReturnCount() > max) {
			// If the odds are decent that we're going to get close to
			// our limit: give up now without additional overhead.

			if(log.isLoggable(Level.INFO))
				log.info("skipping because profile shows average exceeds "+max+": "+request.profile);
			
			return false;
		}
		
		return true;
	}
	
	/**
	 * Return the maximum number of oids we'll cache.
	 */
	protected int getMaxOidListSize(boolean involvedSplit,QueryByCriteria query) {
		return 500;
	}
	
	/**
	 * Return true if we should split an Operator to evaluate its elements.
	 */
	protected boolean isCachingSplit(QueryRequest request) {
		if(!request.orderBy.isSimple()) {
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
	
	static UncaughtExceptionHandler DEFAULT_UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			AppGlobals.getLog().log(Level.SEVERE, "", e);
		}
		
	};
	
	/**
	 * This UncaughtExceptionHandler does nothing; it should be used if you call {@link #setUncaughtExceptionHandler(UncaughtExceptionHandler)} and pass in null.
	 */
	static UncaughtExceptionHandler NULL_UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			//intentionally empty
		}
		
	};
	
	/**
	 * Return the UncaughtExceptionHandler. The default handler writes the stack trace to the AppGlobals log.
	 */
	public UncaughtExceptionHandler getUncaughtExceptionHandler() {
		return uncaughtExceptionHandler;
	}
	
	/**
	 * Assign the UncaughtExceptionHandler.
	 * 
	 * @param ueh the new UncaughtExceptionHandler. If this is null then an empty UncaughtExceptionHandler is used (that does nothing).
	 */
	public void setUncaughtExceptionHandler(UncaughtExceptionHandler ueh) {
		if(ueh==null) ueh = NULL_UNCAUGHT_EXCEPTION_HANDLER;
		uncaughtExceptionHandler = ueh;
	}
	
	/**
	 * Create a BrokerDash that uses this factory's CachePool.
	 * <p>
	 * It is safe to call this method redundantly. If the argument already uses
	 * this factory's ThreadPool then the argument is returned as-is.
	 */
	public BrokerDash convertToBrokerDash(X2Broker broker) {
		validatePersistenceKey(broker.getPersistenceKey());
		
		if (broker instanceof BrokerDash) {
			BrokerDash bd = (BrokerDash) broker;
			Dash sharedResource = bd.getDash();
			if (sharedResource == this)
				return bd;
		}
		return createBrokerDash(broker);
	}

	private void validatePersistenceKey(PersistenceKey otherPersistenceKey) {
		if(persistenceKey!=otherPersistenceKey)
			throw new IllegalStateException(persistenceKey+" != "+otherPersistenceKey);
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
		if(r==null)
			r = log;
		return r;
	}
	
	/**
	 * Set a log for debugging.
	 * 
	 * @param appendable the new Appendable to append output to
	 * @param threadSpecific if true then this Appendable will only be consulted
	 * for the current thread. If false then this Appendable will apply to all threads.
	 */
	public void setLog(Logger log,boolean threadSpecific) {
		if(threadSpecific) {
			logByThread.set(log);
		} else {
			logByThread.set(null);
			this.log = log;
		}
	}

	/**
	 * This is called during {@link X2Broker#rollbackTransaction()} to clear
	 * all cached information related beans that may have been changed during
	 * this rollback.
	 */
	public void clearModifiedBeanTypes() {
		Class[] z;
		synchronized(modifiedBeanTypes) {
			z = modifiedBeanTypes.toArray(new Class[modifiedBeanTypes.size()]);
			modifiedBeanTypes.clear();
		}
		
		for(Class c : z) {
			clearCache(c);
		}
	}

	public PersistenceKey getPersistenceKey() {
		return persistenceKey;
	}

}
