package org.abc.dash;

import com.follett.fsc.core.k12.business.X2Broker;


/**
 * A BrokerDash is an X2Broker that caches some of its operations.
 */
public interface BrokerDash extends X2Broker {

	/**
	 * Multiple BrokerDashes may share the same Dash instance.
	 */
	Dash getDash();

	void setDashActive(boolean active);
	
	boolean isDashActive();
}