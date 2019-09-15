package org.abc.dash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import org.abc.util.OrderByComparator;
import org.apache.ojb.broker.Identity;
import org.apache.ojb.broker.metadata.FieldHelper;
import org.apache.ojb.broker.query.Criteria;

import com.follett.fsc.core.framework.persistence.BeanQuery;
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

public class BrokerDashSharedResource {
	
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
	
	CachePool cachePool;
	Cache<Operator, QueryProfile> profiles;

	Map<Class, Cache<CacheKey, List<String>>> cacheByBeanType;
	
	protected BrokerDashSharedResource(CachePool cachePool) {
		Objects.requireNonNull(cachePool);
		this.cachePool = cachePool;
		profiles = new Cache(cachePool);
		cacheByBeanType = new HashMap<>();
	}

	protected Cache<CacheKey, List<String>> getCache(Class beanClass, boolean createIfMissing) {
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
	 * Create a QueryIteratorDash for a BeanQuery.
	 * <p>
	 * In an ideal case: this will use cached data to completely avoid making a
	 * database query.
	 * <p>
	 * This method should never issue more than one database query. There are 3
	 * database queries this can issue:
	 * <ul><li>The original incoming query as-is.</li>
	 * <li>A query to retrieve beans based on oids. If this caching layer was able to identify
	 * the exact oids we need, but those beans are no longer in Aspen's cache: a query based
	 * on the oids should be pretty efficient.</li>
	 * <li>A query to retrieve a subset of the original query. In this case we were able to
	 * split the original query into smaller pieces, and some of those pieces we could uncache
	 * and others we could not.</li></ul>
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected QueryIteratorDash createCachedIterator(X2Broker broker,BeanQuery beanQuery) {
		
		Operator operator = CriteriaToOperatorConverter
				.createOperator(beanQuery.getCriteria());
		
		Operator template = operator.getTemplateOperator();
		final QueryProfile profile;
		synchronized(profiles) {
			profile = profiles.get(template);
			if(profile==null) {
				profile = new QueryProfile();
				profiles.put(template, profile);
			}
		}
		
		QueryIteratorDash dashIter = createDashIterator(broker, beanQuery, operator, profile);
		dashIter.addCloseListener(profile);
		return dashIter;
	}
	
	protected QueryIteratorDash createDashIterator(X2Broker broker, BeanQuery beanQuery, Operator operator, QueryProfile profile) {
		OrderByComparator orderBy = new OrderByComparator(false,
				beanQuery.getOrderBy());
		
		if(!isCaching(orderBy, profile, beanQuery)) {
			QueryIterator iter = broker.getIteratorByQuery(beanQuery);
			QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), null, iter);
			return dashIter;
		}
		
		Cache<CacheKey, List<String>> cache = getCache(beanQuery.getBaseClass(), true);
		CacheKey cacheKey = new CacheKey(operator, orderBy);
		List<String> beanOids = cache.get(cacheKey);
		
		if (beanOids != null) {
			List<X2BaseBean> beans = getBeansFromGlobalCache(broker.getPersistenceKey(), beanQuery.getBaseClass(), beanOids);
			if(beans!=null) {
				// This is our ideal case: we know the complete query results
				return new QueryIteratorDash(broker.getPersistenceKey(), beans);
			}
			
			// We know the exact oids, but those beans aren't in Aspen's cache 
			// anymore. We can at least rewrite the query:
			
			Criteria oidCriteria = new Criteria();
			oidCriteria.addIn(X2BaseBean.COL_OID, beanOids);
			beanQuery = new BeanQuery(beanQuery.getBaseClass(), oidCriteria);
			for(FieldHelper fieldHelper : orderBy.getFieldHelpers()) {
				beanQuery.addOrderBy(fieldHelper);
			}

			QueryIterator iter = broker.getIteratorByQuery(beanQuery);
			QueryIteratorDash dashIter = new QueryIteratorDash(broker.getPersistenceKey(), null, iter);
			return dashIter;
		}
		
		// we couldn't retrieve the entire query results from our cache

		Operator canonicalOperator = operator.getCanonicalOperator();
		Collection<Operator> splitOperators = canonicalOperator.split();

		if(splitOperators.size() <= 1 || !isCachingSplit(orderBy, profile)) {
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
				return new QueryIteratorDash(broker.getPersistenceKey(), beansToReturn, iter);
			}
			
			// We have all the beans. Cache the oids for next time and return.

			beanOids = new LinkedList<>();
			for(X2BaseBean bean : beansToReturn) {
				beanOids.add(bean.getOid());
			}
			cache.put(cacheKey, beanOids);
			
			return new QueryIteratorDash(broker.getPersistenceKey(), beansToReturn, null);
		}
		
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
			CacheKey splitKey = new CacheKey(splitOperator, orderBy);
			
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
					// This splitOperator is a lost cause now: just treat it like any other query.
					cache.remove(splitKey);
				}
			}
		}
	
		if(splitOperators.isEmpty()) {
			// we broke the criteria down into small pieces and looked up every 
			// piece. We only had to pay the cost of sorting
			return new QueryIteratorDash(broker.getPersistenceKey(), knownBeans);
		} else if(removedOneOrMoreOperators) {
			// we resolved *some* operators, but not all of them.
			Operator trimmedOperator = Operator.join(splitOperators.toArray(new Operator[splitOperators.size()]));
			Criteria trimmedCriteria = CriteriaToOperatorConverter.createCriteria(trimmedOperator);
			
			// ... so we're going to make a new (narrower) query, and merge its results with knownBeans
			beanQuery = new BeanQuery(beanQuery.getBaseClass(), trimmedCriteria);
			for(FieldHelper fieldHelper : orderBy.getFieldHelpers()) {
				beanQuery.addOrderBy(fieldHelper);
			}
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
			return new QueryIteratorDash(broker.getPersistenceKey(), knownBeans, iter);
		}

		beanOids = new LinkedList<>();
		for(X2BaseBean bean : knownBeans) {
			beanOids.add(bean.getOid());
		}
		cache.put(cacheKey, beanOids);
		
		scanOps : for(Operator op : splitOperators) {
			if(isCachingSplit(orderBy, profile, op, knownBeans)) {
				List<String> oids = new LinkedList<>();
		
				for(X2BaseBean bean : knownBeans) {
					try {
						if(op.evaluate(BrokerDash.CONTEXT, bean)) {
							oids.add(bean.getOid());
						}
					} catch(Exception e) {
						AppGlobals.getLog().log(Level.SEVERE, "An error occurred evaluating \""+op+"\" on \""+bean+"\"", e);
						continue scanOps;
					}
				}

				CacheKey splitKey = new CacheKey(op, orderBy);
				cache.put(splitKey, oids);
			}
		}

		return new QueryIteratorDash(broker.getPersistenceKey(), knownBeans);
	}

	protected boolean isCachingSplit(OrderByComparator orderBy,
			QueryProfile profile, Operator op, Collection<X2BaseBean> oidsToEvaluate) {
		if(!orderBy.isSimple())
			return false;
		if(oidsToEvaluate.size()>100)
			return false;
		return true;
	}

	public void clearAll() {
		synchronized(cacheByBeanType) {
			cachePool.clear();
			cacheByBeanType.clear();
		}
	}
	
	protected boolean isCaching(OrderByComparator orderBy, QueryProfile profile, BeanQuery beanQuery) {
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
	protected int getMaxOidListSize(boolean involvedSplit,BeanQuery beanQuery) {
		return 500;
	}
	
	protected boolean isCachingSplit(OrderByComparator orderBy, QueryProfile profile) {
		if(!orderBy.isSimple()) {
			// When you call "myStudent.getPerson().getAddress()", that may
			// involve two separate database queries. So if our order-by comparator
			// involves fetching similar properties: that means we may be issuing
			// lots of queries just to sort beans in the expected order.
			// This defeats the purpose of our caching model: if we saved one
			// BeanQuery but introduced N-many calls to BeanManager#retrieveReference
			// then we may have just made performance (much) worse.
			
			return false;
		}
		
		return true;
	}
}
