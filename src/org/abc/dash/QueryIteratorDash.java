package org.abc.dash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.ojb.broker.PersistenceBroker;
import org.apache.ojb.broker.query.Query;

import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;

class QueryIteratorDash extends QueryIterator {
	
	static interface CloseListener {
		public void closedIterator(int returnCount,boolean hasNext);
	}
	
	Collection<X2BaseBean> beans;
	QueryIterator queryIterator;
	PersistenceKey persistenceKey;
	List<CloseListener> closeListeners = new ArrayList<>();
	
	/**
	 * The number of successful invocations of {@link #next()}.
	 */
	int nextCounter = 0;
	
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
	
	public void addCloseListener(CloseListener l) {
		closeListeners.add(l);
	}
	
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
		X2BaseBean returnValue = beans.iterator().next();
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