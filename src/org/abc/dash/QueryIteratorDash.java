package org.abc.dash;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;

import org.apache.ojb.broker.PersistenceBroker;
import org.apache.ojb.broker.query.Query;

import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;

class QueryIteratorDash extends QueryIterator {
	Collection<X2BaseBean> beans;
	QueryIterator queryIterator;
	PersistenceKey persistenceKey;
	
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
	 * and add it to the collection of beans. Then we pull the first bean from the collection of beans.
	 * <p>
	 * Sometimes the collection of incoming beans is a LinkedList, so this approach will give a FIFO order.
	 * Sometimes the collection of incoming beans is a TreeSet, so this approach will use the TreeSet's
	 * Comparator to merge the results.
	 * 
	 * @param persistenceKey the PersistenceKey associated with this iterator
	 * @param beans the optional collection of beans to walk through
	 * @param queryIterator the optional QueryIterator to walk through
	 */
	public QueryIteratorDash(PersistenceKey persistenceKey, Collection<X2BaseBean> beans, QueryIterator queryIterator) {
		Objects.requireNonNull(persistenceKey);
		this.beans = beans==null ? new LinkedList<X2BaseBean>() : beans;
		this.queryIterator = queryIterator;
		this.persistenceKey = persistenceKey;
	}

	@Override
	protected void finalize() {
		close();
	}

	@Override
	public void close() {
		if(queryIterator!=null)
			queryIterator.close();
		queryIterator = null;
		beans.clear();
	}

	@Override
	protected Iterator getIterator(PersistenceBroker arg0, Query arg1) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object next() {
		if(queryIterator!=null) {
			if(queryIterator.hasNext()) {
				X2BaseBean bean = (X2BaseBean) queryIterator.next();
				beans.add(bean);
			}
			if(!queryIterator.hasNext())
				queryIterator = null;
		}
		return beans.iterator().next();
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