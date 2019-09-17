package org.abc.dash;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import org.abc.tools.Tool;

import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.tools.procedures.ProcedureJavaSource;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.pump.util.Cache.CachePool;

/**
 * This offers a static method to create a BrokerDash.
 */
@Tool(name = "Dash Caching Model", id = "DASH-CACHE")
public class Dash extends ProcedureJavaSource {
	private static final long serialVersionUID = 1L;
	
	/**
	 * This simple class just echoes the signatures of BrokerDashSharedResource.
	 * But because this is a public static class, other Aspen tools can refer to
	 * it from outside the abc package hierarchy.
	 */
	public static class SharedResource extends BrokerDashSharedResource {

		/**
		 * 
		 * @param cacheEntryLimit the maximum number of entries allowed in the cache
		 * @param cacheMillisLimit the maximum duration any entry can stay in the cache.
		 */
		public SharedResource(int cacheEntryLimit, long cacheMillisLimit) {
			super(new CachePool(cacheEntryLimit, cacheMillisLimit, -1));
		}

	}
	
	/**
	 * Create a BrokerDash based on an existing X2Broker.
	 * <p>
	 * The resulting X2Broker sits on top of the original broker and
	 * uses the Dash caching layer to filter requests.
	 * 
	 * @param broker a broker.
	 * @param SharedResource an optional SharedResource
	 * @param paramMap an optional map of key/value pairs that can persist through
	 * the current session. For example: if a multithreaded task creates
	 * n-many ModelBrokers and passes the same Map to each invocation of this
	 * method: then each call will use the same caching layer (BrokerDashFactory).
	 * 
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static X2Broker convertBroker(X2Broker broker, SharedResource sharedResource, Map paramMap) {
		if(paramMap==null)
			paramMap = new HashMap();
		String key = BrokerDashFactory.class.getName();
		BrokerDashFactory factory;
		synchronized(paramMap) {
			factory = (BrokerDashFactory) paramMap.get(key);
			if(factory==null) {
				if(sharedResource!=null) {
					factory = new BrokerDashFactory(sharedResource);
				} else {
					factory = new BrokerDashFactory(5000, 1000*60*20 );
				}
				try {
					paramMap.put(key, factory);
				} catch(Exception e) {
					AppGlobals.getLog().log(Level.SEVERE, "Could not store BrokerDashFactory for reuse.", e);
				}
			}
		}
		return factory.convertToBrokerDash(broker);
	}

	@Override
	protected void execute() throws Exception {
		logMessage("Call Dash.convertBroker(broker, parameters) to use this caching model.");
	}

}
