package org.abc.dash;

import com.follett.fsc.core.k12.business.X2Broker;

/**
 * A BrokerDash is an X2Broker that caches some of its operations and beans.
 */
public interface BrokerDash extends X2Broker {

	/**
	 * Return the Dash object that controls the caching layer.
	 * <p>
	 * Multiple BrokerDashes may share the same Dash instance.
	 */
	Dash getDash();

	/**
	 * Toggle the Dash caching on or off.
	 */
	void setDashActive(boolean active);

	/**
	 * Return whether the Dash caching is active or inactive.
	 * <p>
	 * By default when you first create a BrokerDash caching is
	 * active.
	 */
	boolean isDashActive();
}