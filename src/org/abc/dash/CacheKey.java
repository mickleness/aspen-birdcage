package org.abc.dash;

import java.util.Objects;

import org.abc.util.OrderByComparator;

import com.pump.data.operator.Operator;

public class CacheKey {
	Operator operator;
	OrderByComparator orderBy;
	
	CacheKey(Operator operator,OrderByComparator orderBy) {
		Objects.requireNonNull(orderBy);
		Objects.requireNonNull(operator);
		this.operator = operator;
		this.orderBy = orderBy;
	}

	@Override
	public int hashCode() {
		return operator.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof CacheKey))
			return false;
		CacheKey other = (CacheKey) obj;
		if(!operator.equals(other.operator))
			return false;
		if(!orderBy.equals(other.orderBy))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CacheKey[ operator=\""+operator+"\", orderBy=\""+orderBy+"\"]";
	}
}