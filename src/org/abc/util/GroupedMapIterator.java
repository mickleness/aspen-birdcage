package org.abc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This iterates over clusters of maps that share a common break value.
 * <p>
 * For example, consider the following maps:
 *
 * <pre>
 * name=Kim, grade=10
 * name=Molly, grade=10
 * name=Daniel, grade=11
 * </pre>
 *
 * If you create a MapIterator and pass in a breakCriteria of "grade", then this
 * will return two lists of maps: the first map will include Kim and Molly, and
 * the second list will have Daniel as its only element.
 *
 */
public class GroupedMapIterator implements Iterator<Collection<Map>> {

	List<Map> next = null;
	Map unusedMap = null;
	Iterator<Map> src;
	Object breakCriteria;
	Collection<Object> previousBreakValues = new HashSet<>();

	public GroupedMapIterator(Iterable<Map> src, Object breakCriteria) {
		this(src.iterator(), breakCriteria);
	}

	public GroupedMapIterator(Iterator<Map> src, Object breakCriteria) {
		this.breakCriteria = breakCriteria;
		this.src = src;
		if (src.hasNext()) {
			unusedMap = src.next();
		}
		queueNext();
	}

	private void queueNext() {
		if (unusedMap == null) {
			next = null;
			return;
		}

		next = new ArrayList<>();
		Object breakValue = unusedMap.get(breakCriteria);
		if (!previousBreakValues.add(breakValue))
			throw new RuntimeException(
					"The break value \""
							+ breakValue
							+ "\" was already by a non-adjacent map. The source iterator for this data is out-of-order. All maps should be sorted according to the break criteria ("
							+ breakCriteria + ").");
		next.add(unusedMap);
		unusedMap = null;
		while (src.hasNext()) {
			Map map = src.next();
			Object value2 = map.get(breakCriteria);
			if (Objects.equals(breakValue, value2)) {
				next.add(map);
			} else {
				unusedMap = map;
				return;
			}
		}
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public List<Map> next() {
		List<Map> returnValue = next;
		queueNext();
		return returnValue;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
