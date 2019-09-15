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

	Map<Class, Cache<CacheKey, Object>> cacheByBeanType;
	
	public BrokerDashSharedResource(CachePool cachePool) {
		Objects.requireNonNull(cachePool);
		this.cachePool = cachePool;	
		cacheByBeanType = new HashMap<Class, Cache<CacheKey, Object>>();
	}

	public Cache<CacheKey, Object> getCache(Class beanClass, boolean createIfMissing) {
		synchronized(cacheByBeanType) {
			Cache<CacheKey, Object> cache = cacheByBeanType.get(beanClass);
			if(cache==null && createIfMissing) {
				cache = new Cache<>(cachePool);
				cacheByBeanType.put(beanClass, cache);
			}
			return cache;
		}
	}


	/**
	 * Create a QueryIterator for a BeanQuery.
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
	protected QueryIterator createdCachedIterator(X2Broker broker,BeanQuery beanQuery) {
		Operator operator = CriteriaToOperatorConverter
				.createOperator(beanQuery.getCriteria());
		OrderByComparator orderBy = new OrderByComparator(false,
				beanQuery.getOrderBy());
		CacheKey cacheKey = new CacheKey(operator, orderBy);
		
		Cache<CacheKey, Object> cache = getCache(beanQuery.getBaseClass(), true);

		Object cacheValue = cache.get(cacheKey);
		List<String> beanOids;
		boolean cacheResults;
		if(cacheValue instanceof List) {
			beanOids = (List<String>) cacheValue;
			cacheResults = false;
		} else if(cacheValue instanceof AtomicInteger) {
			AtomicInteger i = (AtomicInteger) cacheValue;
			if(i.intValue()<0) {
				cacheResults = false;
			} else {
				cacheResults = i.incrementAndGet() > 5;
			}
			beanOids = null;
		} else {
			cache.put(cacheKey, new AtomicInteger(0));
			beanOids = null;
			cacheResults = false;
		}
		
		if (beanOids != null) {
			List<X2BaseBean> beans = getBeansFromGlobalCache(broker.getPersistenceKey(), beanQuery.getBaseClass(), beanOids);
			if(beans!=null) {
				//this is our ideal case: we know the complete query results
				return new QueryIteratorDash(broker.getPersistenceKey(), beans);
			}
			
			// we know the exact oids, but those beans aren't in Aspen's cache anymore.
			
			// we can at least rewrite the query:
			Criteria oidCriteria = new Criteria();
			oidCriteria.addIn(X2BaseBean.COL_OID, beanOids);
			beanQuery = new BeanQuery(beanQuery.getBaseClass(), oidCriteria);
			for(FieldHelper fieldHelper : orderBy.getFieldHelpers()) {
				beanQuery.addOrderBy(fieldHelper);
			}
			return broker.getIteratorByQuery(beanQuery);
		}

		Operator canonicalOperator = operator.getCanonicalOperator();
		Collection<X2BaseBean> knownBeans;
		if(orderBy.getFieldHelpers().isEmpty()) {
			//order doesn't matter!
			knownBeans = new LinkedList<>();
		} else {
			knownBeans = new TreeSet<>(orderBy);
		}
		
		//if a criteria said: "A==1 or A==2", then this splits them into two
		//unique criteria "A==1" and "A==2"
		Collection<Operator> splitOperators = canonicalOperator.split();
		List<Operator> splitOperatorsToCache = new LinkedList<>();
		if(splitOperators.size()>1) {
			Iterator<Operator> splitIter = splitOperators.iterator();
			
			boolean removedOperators = false;
			while(splitIter.hasNext()) {
				Operator splitOperator = splitIter.next();
				CacheKey splitKey = new CacheKey(splitOperator, orderBy);
				
				Object splitCacheValue = cache.get(splitKey);
				List<String> splitOids = null;
				if(splitCacheValue instanceof List) {
					splitOids = (List<String>) splitCacheValue;

					List<X2BaseBean> splitBeans = getBeansFromGlobalCache(broker.getPersistenceKey(), 
							beanQuery.getBaseClass(), 
							splitOids);
					if(splitBeans!=null) {
						removedOperators = true;
						knownBeans.addAll(splitBeans);
						splitIter.remove();
					} else {
						// we know the exact oids, but those beans aren't in Aspen's cache anymore.
						// this is a shame, but there's nothing this caching layer can do to help.
					}
				} else if(splitCacheValue instanceof AtomicInteger) {
					AtomicInteger i = (AtomicInteger) splitCacheValue;
					int attempts = i.get();
					if(attempts<5) {
						i.incrementAndGet();
					} else if(attempts<10) {
						splitOperatorsToCache.add(splitOperator);
					}
				} else {
					cache.put(splitKey, new AtomicInteger(0));
				}
			}
		
			if(splitOperators.isEmpty()) {
				// we broke the criteria down into small pieces and looked up every 
				// piece. We only had to pay the cost of sorting (which might include
				// its own queries)
				return new QueryIteratorDash(broker.getPersistenceKey(), knownBeans);
			} else if(removedOperators) {
				// we resolved *some* operators, but not all of them.
				Operator trimmedOperator = Operator.join(splitOperators.toArray(new Operator[splitOperators.size()]));
				Criteria trimmedCriteria = CriteriaToOperatorConverter.createCriteria(trimmedOperator);
				
				// so we're going to make a new (narrower) query, and merge its results with knownBeans
				beanQuery = new BeanQuery(beanQuery.getBaseClass(), trimmedCriteria);
				for(FieldHelper fieldHelper : orderBy.getFieldHelpers()) {
					beanQuery.addOrderBy(fieldHelper);
				}
			}
		}
		
		if(knownBeans.isEmpty()) {
			// we have a blank slate (no cached info came up)
			
			// so let's just dump incoming beans in a list. The order is going to be correct,
			// because the order is coming straight from the source. So there's no need
			// to use a TreeSet with a comparator anymore:
			
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
			// too many beans; let's give up on caching.
			
			// if knownBeans is a TreeSet: this is our worst-case scenario
			// Here will keep consulting the TreeSet's comparator for every
			// new bean, which may be expensive.
			return new QueryIteratorDash(broker.getPersistenceKey(), knownBeans, iter);
		}
		
		// we exhausted the QueryIterator, so we have all the beans we need to return.
		
		// ... but before we return let's cache everything for future lookups:
		
		if(cacheResults) {
			List<String> knownBeanOids = new LinkedList<>();
			for(X2BaseBean bean : knownBeans) {
				knownBeanOids.add(bean.getOid());
			}
			cache.put(cacheKey, knownBeanOids);
		}
		
		if(!splitOperatorsToCache.isEmpty()) {
			Map<Operator, List<String>> oidsByOperator = new HashMap<>();
			scanOps : for(Operator op : splitOperatorsToCache) {
				List<String> oids = oidsByOperator.get(op);
				if(oids==null) {
					oids = new LinkedList<>();
					oidsByOperator.put(op, oids);
				}
				for(X2BaseBean bean : knownBeans) {
					try {
						if(op.evaluate(BrokerDash.CONTEXT, bean)) {
							oids.add(bean.getOid());
						}
					} catch(Exception e) {
						AppGlobals.getLog().log(Level.SEVERE, "An error occurred evaluated \""+op+"\" on \""+bean+"\"", e);
						// store a large value in here so we'll never try caching this particular Operator again
						cache.put(new CacheKey(op, orderBy), new AtomicInteger(100));
						oidsByOperator.remove(op);
						continue scanOps;
					}
				}
			}
			
			for(Entry<Operator, List<String>> entry : oidsByOperator.entrySet()) {
				CacheKey splitKey = new CacheKey(entry.getKey(), orderBy);
				cache.put(splitKey, entry.getValue());
			}
		}

		return new QueryIteratorDash(broker.getPersistenceKey(), knownBeans);
	}

	public void clearAll() {
		synchronized(cacheByBeanType) {
			cachePool.clear();
			cacheByBeanType.clear();
		}
	}
}
