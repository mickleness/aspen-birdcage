package org.abc.dash;

import com.follett.fsc.core.k12.business.X2Broker;
import com.pump.util.Cache.CachePool;

/**
 * A BrokerDash is an X2Broker that caches some of its operations.
 */
public interface BrokerDash extends X2Broker {

	/**
	 * The CachePool any intermediate caches should use.
	 */
	CachePool getCachePool();
}
