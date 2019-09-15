package org.abc.dash;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.beanutils.PropertyUtils;

import com.follett.fsc.core.k12.business.X2Broker;
import com.pump.data.operator.OperatorContext;
import com.pump.util.Cache.CachePool;
import com.x2dev.utils.StringUtils;

/**
 * A BrokerDash is an X2Broker that caches some of its operations.
 */
public interface BrokerDash extends X2Broker {

	/**
	 * This is an OperatorContext that connects Operators to X2BaseBeans.
	 */
	OperatorContext CONTEXT = new OperatorContext() {

		@Override
		public Object getValue(Object dataSource, String attributeName) {

			abstract class Function {
				public abstract Object evaluate(Object input);
			}

			class Parser {
				LinkedList<Function> functions = new LinkedList<>();
				String attributeName;

				public Parser(String input) {
					while (true) {
						if (input.startsWith("upper(") && input.endsWith(")")) {
							functions.add(new Function() {

								@Override
								public Object evaluate(Object input) {
									if (input == null)
										return null;
									return ((String) input).toUpperCase();
								}

							});
							input = input.substring("upper(".length(),
									input.length() - 1);
						} else if (input.startsWith("ISNUMERIC(")
								&& input.endsWith(")")) {
							functions.add(new Function() {

								@Override
								public Object evaluate(Object input) {
									if (input == null)
										return null;
									return StringUtils
											.isNumeric((String) input);
								}

							});
							input = input.substring("ISNUMERIC(".length(),
									input.length() - 1);
						} else {
							attributeName = input;
							return;
						}
					}
				}

			}

			Parser parser = new Parser(attributeName);

			Object value;
			try {
				value = PropertyUtils.getProperty(dataSource,
						parser.attributeName);
			} catch (IllegalAccessException | InvocationTargetException
					| NoSuchMethodException e) {
				throw new RuntimeException("An error occurred retrieved \""
						+ attributeName + "\" from " + dataSource, e);
			}
			Iterator<Function> iter = parser.functions.descendingIterator();
			while (iter.hasNext()) {
				value = iter.next().evaluate(value);
			}
			return value;
		}

	};

	/**
	 * Multiple BrokerDashes may share the same BrokerDashSharedResource.
	 */
	BrokerDashSharedResource getSharedResource();
}
