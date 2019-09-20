package org.abc.dash;

import org.apache.ojb.broker.query.Criteria;

import com.pump.data.operator.Operator;

/**
 * This converts org.apache.ojb.broker.query.Criterias into 
 * com.pump.data.operator.Operators (and back again).
 */
public interface CriteriaToOperatorConverter {

	/**
	 * Convert an Operator into a Criteria.
	 */
	public Criteria createCriteria(Operator operator);

	/**
	 * Convert a Criteria into an Operator.
	 * <p>
	 * This may throw an exception if this uses a criteria feature that is not
	 * supported by Operators.
	 */
	public Operator createOperator(Criteria criteria);
}
