package org.abc.dash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.abc.tools.ThreadedBrokerIterator;
import org.apache.ojb.broker.PersistenceBroker;
import org.apache.ojb.broker.query.Query;

import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;

/**
 * This iterates over a combination of pre-loaded objects and objects dynamically coming in
 * from a live QueryIterator.
 * <p>
 * This iterator is not thread-safe; it is assumed this will only be used on
 * one thread.
 */
public class QueryIteratorDash<T> extends QueryIterator<T> {

	/**
	 * This listener is notified when a QueryIteratorDash closes.
	 */
	public static interface CloseListener {
		/**
		 * This method is invoked when an iterator closes.
		 * 
		 * @param returnCount
		 *            the number of times this iterator produced a value.
		 * @param hasNext
		 *            whether this iterator's {@link Iterator#hasNext()} method
		 *            indicated there were results remaining when this iterator
		 *            was closed.
		 */
		public void closedIterator(int returnCount, boolean hasNext);
	}

	/**
	 * This is never null, although it may be empty.
	 */
	protected Collection<T> elements;
	
	protected boolean isClosed = false;
	protected boolean frozenHasNext = false;
	
	/**
	 * This may always be null, or it may become null on exhaustion.
	 */
	protected QueryIterator<T> queryIterator;

	protected Dash dash;
	protected List<CloseListener> closeListeners = new ArrayList<>();

	/**
	 * The number of successful invocations of {@link #next()}.
	 */
	protected int nextCounter = 0;

	/**
	 * Create an iterator that will walk through a collection of elements.
	 * 
	 * @param dash
	 *            this is used to identify the persistence key and possibly to
	 *            cache beans by their oid.
	 */
	public QueryIteratorDash(Dash dash, Collection<T> elements) {
		this(dash, elements, null);
		// if you use this constructor: you should give us something to iterate
		// over
		Objects.requireNonNull(elements);
	}

	/**
	 * Create an iterator that will walk through a collection of elements and a
	 * QueryIterator.
	 * <p>
	 * Every time a new element is requested: if possible we pull an element from the
	 * QueryIterator and add it to the collection of elements. Then we return the
	 * first element from the collection of preloaded elements.
	 * <p>
	 * So if the collection is a List, this approach will generate a FIFO
	 * iterator. If the collection is a SortedSet, then this approach will merge
	 * the QueryIterator with the SortedSet. (This assumes the QueryIterator
	 * will return results using the same order-by rules that the SortedSet
	 * follows.)
	 * 
	 * @param dash
	 *            this is used to identify the persistence key and possibly to
	 *            cache beans by their oid.
	 * @param elements
	 *            an optional collection of elements to walk through. This may be
	 *            null.
	 * @param queryIterator
	 *            the optional QueryIterator to walk through. This may be null.
	 */
	public QueryIteratorDash(Dash dash, Collection<T> elements,
			QueryIterator<T> queryIterator) {
		Objects.requireNonNull(dash);
		this.elements = elements == null ? new LinkedList<T>() : elements;
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
		if(isClosed())
			return;

		isClosed = true;
		frozenHasNext = hasNext();

		if (queryIterator != null)
			queryIterator.close();
		queryIterator = null;
		elements.clear();

		for (CloseListener listener : closeListeners
				.toArray(new CloseListener[closeListeners.size()])) {
			listener.closedIterator(nextCounter, frozenHasNext);
		}
	}

	@Override
	protected Iterator<T> getIterator(PersistenceBroker arg0, Query arg1) {
		throw new UnsupportedOperationException();
	}

	@Override
	public T next() {
		ThreadedBrokerIterator.checkInterruptNoYield();
		if(isClosed())
			throw new NoSuchElementException("This iterator is closed.");

		if (queryIterator != null) {
			if (queryIterator.hasNext()) {
				T element = queryIterator.next();
				if(element instanceof X2BaseBean)
					dash.storeBean((X2BaseBean) element);
				elements.add(element);
			}
			if (!queryIterator.hasNext()) {
				queryIterator.close();
				queryIterator = null;
			}
		}

		if (elements.size() == 0)
			throw new NoSuchElementException();

		Iterator<T> elementsIter = elements.iterator();
		T returnValue = elementsIter.next();
		elementsIter.remove();
		nextCounter++;
		return returnValue;
	}

	@Override
	public boolean hasNext() {
		if(isClosed())
			return frozenHasNext;
		
		if (!elements.isEmpty())
			return true;
		if (queryIterator != null && queryIterator.hasNext())
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
	
	/**
	 * Return true if {@link #isClosed()} has been called.
	 * @return
	 */
	public boolean isClosed() {
		return isClosed;
	}
}