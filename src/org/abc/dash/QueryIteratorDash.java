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

/**
 * This iterates over a series of X2BaseBeans.
 * <p>
 * This may pull data from two sources: a predefined collection of
 * beans and/or a QueryIterator.
 */
public class QueryIteratorDash extends QueryIterator<X2BaseBean> {
	
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
	
	protected Dash dash;
	protected List<CloseListener> closeListeners = new ArrayList<>();
	
	/**
	 * The number of successful invocations of {@link #next()}.
	 */
	protected int nextCounter = 0;
	
	/**
	 * Create an iterator that will walk through a collection of beans.
	 * 
	 * @param dash this is used to identify the persistence key and possibly to cache beans by their oid.
	 */
	public QueryIteratorDash(Dash dash,Collection<X2BaseBean> beans) {
		this(dash, beans, null);
		//if you use this constructor: you should give us something to iterate over
		Objects.requireNonNull(beans);
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
	 * @param dash this is used to identify the persistence key and possibly to cache beans by their oid.
	 * @param beans an optional collection of beans to walk through. This may be null.
	 * @param queryIterator the optional QueryIterator to walk through. This may be null.
	 */
	public QueryIteratorDash(Dash dash, Collection<X2BaseBean> beans, QueryIterator<X2BaseBean> queryIterator) {
		Objects.requireNonNull(dash);
		this.beans = beans==null ? new LinkedList<X2BaseBean>() : beans;
		this.queryIterator = queryIterator;
		this.dash = dash;
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
				dash.storeBean(bean);
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
		return dash.getPersistenceKey();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}