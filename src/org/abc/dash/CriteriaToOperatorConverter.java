package org.abc.dash;

import java.util.Collection;
import java.util.Enumeration;

import org.apache.ojb.broker.query.BetweenCriteria;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.LikeCriteria;
import org.apache.ojb.broker.query.ValueCriteria;

import com.pump.data.operator.And;
import com.pump.data.operator.EqualTo;
import com.pump.data.operator.GreaterThan;
import com.pump.data.operator.In;
import com.pump.data.operator.LesserThan;
import com.pump.data.operator.Like;
import com.pump.data.operator.Not;
import com.pump.data.operator.Operator;
import com.pump.data.operator.Or;
import com.pump.text.WildcardPattern;

public class CriteriaToOperatorConverter {
	// TODO: add support for X2Criteria#addEqualToIgnoreCase, and anything else
	// that comes up.

	static WildcardPattern.Format sqlFormat = new WildcardPattern.Format();
	static {
		sqlFormat.closeBracketCharacter = null;
		sqlFormat.openBracketCharacter = null;
		sqlFormat.escapeCharacter = LikeCriteria.getEscapeCharacter();
		sqlFormat.questionMarkWildcard = '_';
		sqlFormat.starWildcard = '%';
	}

	/**
	 * Convert an Operator into a Criteria.
	 */
	public static Criteria createCriteria(Operator operator) {
		Criteria c = new Criteria();
		if (operator instanceof Not) {
			Operator sub = (Operator) operator.getOperand(0);
			if (sub instanceof EqualTo) {
				EqualTo e = (EqualTo) sub;
				if (e.getValue() == null) {
					c.addNotNull(e.getAttribute());
				} else {
					c.addNotEqualTo(e.getAttribute(), e.getValue());
				}
			} else if (sub instanceof GreaterThan) {
				GreaterThan gt = (GreaterThan) sub;
				c.addLessOrEqualThan(gt.getAttribute(), gt.getValue());
			} else if (sub instanceof LesserThan) {
				LesserThan lt = (LesserThan) sub;
				c.addGreaterOrEqualThan(lt.getAttribute(), lt.getValue());
			} else if (sub instanceof In) {
				In i = (In) sub;
				c.addNotIn(i.getAttribute(), i.getValue());
			} else if (sub instanceof Like) {
				Like l = (Like) sub;
				c.addNotLike(l.getAttribute(), l.getValue().getPatternText());
			} else if (sub instanceof And) {
				And and = (And) sub;
				for (int a = 0; a < and.getOperandCount(); a++) {
					Operator x = and.getOperand(a);
					if (x instanceof Not) {
						x = (Operator) x.getOperand(0);
					} else {
						x = new Not(x);
					}
					Criteria negatedOr = createCriteria(x);
					c.addOrCriteria(negatedOr);
				}
			} else if (sub instanceof Or) {
				Or or = (Or) sub;
				for (int a = 0; a < or.getOperandCount(); a++) {
					Operator x = or.getOperand(a);
					if (x instanceof Not) {
						x = (Operator) x.getOperand(0);
					} else {
						x = new Not(x);
					}
					Criteria negatedAnd = createCriteria(x);
					c.addAndCriteria(negatedAnd);
				}
			} else {
				throw new IllegalArgumentException("Unsupporter operator: "
						+ operator.getClass().getName() + " \"" + operator
						+ "\"");
			}
		} else if (operator instanceof EqualTo) {
			EqualTo e = (EqualTo) operator;
			if (e.getValue() == null) {
				c.addIsNull(e.getAttribute());
			} else {
				c.addEqualTo(e.getAttribute(), e.getValue());
			}
		} else if (operator instanceof GreaterThan) {
			GreaterThan gt = (GreaterThan) operator;
			c.addGreaterThan(gt.getAttribute(), gt.getValue());
		} else if (operator instanceof LesserThan) {
			LesserThan lt = (LesserThan) operator;
			c.addLessThan(lt.getAttribute(), lt.getValue());
		} else if (operator instanceof In) {
			In i = (In) operator;
			c.addIn(i.getAttribute(), i.getValue());
		} else if (operator instanceof Like) {
			Like l = (Like) operator;
			c.addLike(l.getAttribute(), l.getValue().getPatternText());
		} else if (operator instanceof And) {
			And and = (And) operator;
			for (int a = 0; a < and.getOperandCount(); a++) {
				Criteria andC = createCriteria(and.getOperand(a));
				c.addAndCriteria(andC);
			}
		} else if (operator instanceof Or) {
			Or or = (Or) operator;
			for (int a = 0; a < or.getOperandCount(); a++) {
				Criteria orC = createCriteria(or.getOperand(a));
				c.addOrCriteria(orC);
			}
		} else {
			throw new IllegalArgumentException("Unsupporter operator: "
					+ operator.getClass().getName() + " \"" + operator + "\"");
		}
		return c;
	}

	/**
	 * Convert a Criteria into an Operator.
	 * <p>
	 * This may throw an exception if this uses a criteria feature that is not
	 * supported by Operators.
	 */
	public static Operator createOperator(Criteria criteria) {
		Enumeration e = criteria.getElements();
		Operator current = null;
		while (e.hasMoreElements()) {
			Object z = e.nextElement();
			Operator op = null;
			if (z instanceof ValueCriteria) {
				ValueCriteria vc = (ValueCriteria) z;
				BetweenCriteria bc = vc instanceof BetweenCriteria ? (BetweenCriteria) vc
						: null;
				String clause = vc.getClause();
				// these Strings are copied and pasted from protected constants
				// in SelectionCriteria
				switch (clause) {
				case " = ":
					op = new EqualTo((String) vc.getAttribute(),
							(Comparable) vc.getValue());
					break;
				case " <> ":
					op = new Not(new EqualTo((String) vc.getAttribute(),
							(Comparable) vc.getValue()));
					break;
				case " > ":
					op = new GreaterThan((String) vc.getAttribute(),
							(Comparable) vc.getValue());
					break;
				case " <= ":
					op = new Not(new GreaterThan((String) vc.getAttribute(),
							(Comparable) vc.getValue()));
					break;
				case " < ":
					op = new LesserThan((String) vc.getAttribute(),
							(Comparable) vc.getValue());
					break;
				case " >= ":
					op = new Not(new LesserThan((String) vc.getAttribute(),
							(Comparable) vc.getValue()));
					break;
				case " LIKE ":
					op = new Like((String) vc.getAttribute(),
							new WildcardPattern((String) vc.getValue(),
									sqlFormat));
					break;
				case " NOT LIKE ":
					op = new Not(new Like((String) vc.getAttribute(),
							new WildcardPattern((String) vc.getValue(),
									sqlFormat)));
					break;
				case " IS NULL ":
					op = new EqualTo((String) vc.getAttribute(), null);
					break;
				case " IS NOT NULL ":
					op = new Not(new EqualTo((String) vc.getAttribute(), null));
					break;
				case " BETWEEN ":
					op = new And(new GreaterThan((String) bc.getAttribute(),
							(Comparable) bc.getValue()), new LesserThan(
							(String) bc.getAttribute(),
							(Comparable) bc.getValue2()));
					break;
				case " NOT BETWEEN ":
					op = new Or(new LesserThan((String) bc.getAttribute(),
							(Comparable) bc.getValue()), new GreaterThan(
							(String) bc.getAttribute(),
							(Comparable) bc.getValue2()));
					break;
				case " IN ":
					op = In.create((String) vc.getAttribute(),
							(Collection<?>) vc.getValue());
					break;
				case " NOT IN ":
					op = new Not(In.create((String) vc.getAttribute(),
							(Collection<?>) vc.getValue()));
					break;
				}

				if (op == null)
					throw new IllegalArgumentException(
							"Unsupported criteria element: " + z + " ("
									+ z.getClass().getName() + ")");

				if (current == null) {
					current = op;
				} else {
					current = new And(current, op);
				}
			} else if (z instanceof Criteria) {
				Criteria c2 = (Criteria) z;
				op = createOperator((Criteria) z);
				if (current == null) {
					current = op;
				} else if (c2.getType() == Criteria.AND) {
					current = new And(current, op);
				} else if (c2.getType() == Criteria.OR) {
					current = new Or(current, op);
				} else {
					throw new IllegalArgumentException(
							"Unsupported Criteria type " + c2.getType());
				}
			} else {
				if (op == null)
					throw new IllegalArgumentException(
							"Unsupported criteria element: " + z + " ("
									+ z.getClass().getName() + ")");
			}
		}
		return current;
	}
}
