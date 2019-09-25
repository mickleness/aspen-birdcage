package org.abc.dash;

import java.io.IOException;
import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.abc.tools.Tool;
import org.abc.util.BasicEntry;
import org.abc.util.OrderByComparator;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.ojb.broker.Identity;
import org.apache.ojb.broker.metadata.FieldHelper;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.Query;
import org.apache.ojb.broker.query.QueryByCriteria;

import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.X2ObjectCache;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.ModelBroker;
import com.follett.fsc.core.k12.business.ModelProperty;
import com.follett.fsc.core.k12.business.PrivilegeSet;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.pump.data.operator.And;
import com.pump.data.operator.EqualTo;
import com.pump.data.operator.Operator;
import com.pump.data.operator.OperatorContext;
import com.pump.util.Cache;
import com.pump.util.Cache.CachePool;
import com.x2dev.utils.LoggerUtils;
import com.x2dev.utils.StringUtils;

/**
 * The Dash object maintains a cache and a set of shared methods/tools
 * that one or more BrokerDashes can use.
 * <p>
 * This class is thread-safe. So you can set up one Dash object and use it
 * across multiple threads. For example: if you set up n-many threads and 
 * each thread has a unique X2Broker, then the same Dash object can convert
 * that broker into a caching BrokerDash.
 */
@Tool(name = "Dash Caching Model", id = "DASH-CACHE", type="Procedure")
public class Dash {
	
	public static interface BrokerIteratorFunction<Input, Output> {
		public Output apply(X2Broker broker,Input input);
	}
	
	/**
	 * This iterates over a query and passes the query results to separate threads to speed up processing.
	 *
	 * @param <Input> the results of the Query, which are passed to separate threads to be evaluated in a QueryIteratorFunction
	 * @param <Output> the output of a QueryIteratorFunction, which is passed back to the original thread for possible consumption.
	 */
	public static class ThreadedIteratorHelper<Input, Output> {
		
		/**
		 * This is one of the helper threads that listens for inputs and applies the function.
		 * <p>
		 * Helper threads need to be 100% sure they will exit. (We don't want hundreds of orphaned threads
		 * on the server someday.) This thread has three possible conditions under which it will exit:
		 * <ul><li>When completedIterator is toggled from false to true. This is the ideal/typical option.</li>
		 * <li>When {@link #isPollTimeout(long)} indicates grabbing a new Input off the queue has 
		 * taken too long. This generally shouldn't happen unless something else is frozen/blocked. If all helper 
		 * threads shut down because something just naturally took a VERY long time to complete, then the 
		 * ThreadedIteratorHelper will also eventually shut down when it times out after attempting to pass
		 * Inputs off to the helper threads.</li>
		 * <li>When the original tool's thread is no longer alive. This is considered a fail-safe. This
		 * should never be needed, because the completedIterator boolean should be safely changed inside
		 * a finally block.</li></ul>
		 * <p>
		 */
		class HelperThread extends Thread {
			Thread masterThread;
			
			public HelperThread(int index) {
				super(Thread.currentThread().getName()+"-helper-"+index);
				masterThread = Thread.currentThread();
			}

			@Override
        	public void run() {
				long lastInput = System.currentTimeMillis();
				boolean keepRunning = true;
				boolean inputQueueStoppedAcceptingAdditions = false;
        		while(keepRunning) {
        			try {
            			if(!masterThread.isAlive()) {
        					keepRunning = false;
	        				throw new RuntimeException("Aborting "+getName()+" because the master thread ("+masterThread.getName()+") is not alive.");
            			}
            			
            			// I originally tried calls like inputQueue.poll(50, TimeUnit.MILLIS), but for some reason
            			// when I used those calls and killed the master tool: this helper thread seemed to get
            			// locked and never recover. I didn't figure out exactly why, but switching to this
            			// poll() method appeared to resolve it:
            			Input input = inputQueue.poll();
	        			
	        			if(input!=null) {
	        				lastInput = System.currentTimeMillis();
		        			Output output = function.apply(createBroker(), input);
		        			if(output!=null) {
			        			synchronized(outputQueue) {
			        				outputQueue.add(output);
			        			}
		        			}
	        			} else {
	        				if(inputQueueStoppedAcceptingAdditions) {
	        					// First we noticed completedIterator is true, then we 
	        					// drained the inputQueue. We can end this thread now.
	        					return;
	        				}
	        				
	        				long elapsed = System.currentTimeMillis() - lastInput;
	        				if(isPollTimeout(elapsed)) {
	        					keepRunning = false;
		        				throw new RuntimeException("Unexpectedly slow input queue: "+elapsed+" ms");
	        				}
	        				
	        				if(completedIterator.get()) {
	        					inputQueueStoppedAcceptingAdditions = true;
	        				}
	        			}
        			} catch(Exception e) {
        				handleUncaughtException(e);
        			}
        		}
        	}
		}
		
		PrivilegeSet privilegeSet;
		BrokerIteratorFunction<Input, Output> function;
		Consumer<Output> outputListener;
        ArrayBlockingQueue<Input> inputQueue;
        Dash dash;
        int threadCount;
        List<Output> outputQueue = new LinkedList<>();
        AtomicBoolean completedIterator = new AtomicBoolean(false);
        Thread[] threads;

        /**
         * Create a new ThreadedIteratorHelper.
         * 
		 * @param privilegeSet a PrivilegeSet used to create additional ModelBrokers.
		 * @param function a function that accepts the query's iterative values and produces output. This function
		 *        will probably be called on helper threads, but if threadCount is 0 then this function will be called
		 *        on this thread. The broker this function receives as an argument will always be a new (unused)
		 *        X2Broker. It is the function's responsibility to set up transactions appropriately.
		 * @param threadCount the number of threads to use to evaluate this query's results. This must be zero or greater.
		 * Each thread can have a unique X2Broker, and each broker can borrow a database connection: so think of this number
		 * as approximately equal to the number of database connections you want to have active at any give time.
		 * If this is zero then the iterator results are processed immediately and no new threads are created.
		 * @param dash if this is non-null then all new X2Brokers this manager creates will be converted to BrokerDash's so they
		 * may benefit from caching.
		 * @param outputConsumer an optional consumer. If this is non-null, then this consumer is given all the Outputs the
		 * function creates. The consumer is only notified on the master thread that originally invoked this method.
         */
		public ThreadedIteratorHelper(PrivilegeSet privilegeSet,
				BrokerIteratorFunction<Input, Output> function, 
				int threadCount,Dash dash,Consumer<Output> outputListener) {
			Objects.requireNonNull(dash);
			Objects.requireNonNull(privilegeSet);
			Objects.requireNonNull(function);
			if(!(threadCount>=0))
				throw new IllegalArgumentException("threadCount ("+threadCount+") must be at least zero");
			this.privilegeSet = privilegeSet;
			this.function = function;
			this.dash = dash;
			this.threadCount = threadCount;
			inputQueue = threadCount>0 ? new ArrayBlockingQueue<Input>(threadCount*2) : null;
			threads = new Thread[threadCount];
			for(int a = 0; a<threads.length; a++) {
				threads[a] = new HelperThread(a);
				threads[a].start();
			}
			this.outputListener = outputListener;
		}
		
		/**
		 * Execute the input query.
		 * <p>
		 * This should be called from a tool's primary thread.
		 * 
		 * @param broker the original (master) broker used to iterate over the input query.
		 * @param query the query to pass to the broke.
		 */
		public void run(X2Broker broker, Query query) {
			Objects.requireNonNull(broker);
			Objects.requireNonNull(query);
			
			if(completedIterator.get())
				throw new IllegalStateException("The run() method should only be invoked once.");
			
			QueryIterator iter = broker.getIteratorByQuery(query);
			run(iter);
		}
		
		/**
		 * Iterate over all the elements in an iterator.
		 * <p>
		 * This should be called from a tool's primary thread.
		 * 
		 * @param iter the iterator to walk through. If this is an AutoCloseable then it is
		 * automatically closed on completion.
		 */
		public synchronized void run(Iterator iter) {
			if(completedIterator.get())
				throw new IllegalStateException("The run() method should only be invoked once.");
			
			try(AutoCloseable z = getAutoCloseable(iter)) {
				int elementCtr = 0;
				while(iter.hasNext()) {
					Input value = (Input) iter.next();
					
					if(inputQueue==null) {
						Output output = function.apply(createBroker(), value);
						if(output!=null)
							outputQueue.add(output);
					} else {
		        		long t = System.currentTimeMillis();
		        		while(true) {
		        			long elapsed = System.currentTimeMillis() - t;
		        			if(isSubmitTimeout(elapsed))
		        				throw new RuntimeException("Unexpectedly slow queue: "+elapsed+" ms, elementCtr = "+elementCtr);
		        			try {
			        			if(inputQueue.offer( value, 5, TimeUnit.SECONDS))
			        				break;
		        			} catch(InterruptedException e) {
		        				//intentionally empty
		        			}
		        		}
					}
					
					flushOutputs();
	        		
	        		elementCtr++;
				}
			} catch(RuntimeException e) {
				throw e;
			} catch(Exception e) {
				handleUncaughtException(e);
			} finally {
				completedIterator.set(true);
			}
			
			//let our helper threads drain the input queue:
			
			while(getHelperThreadCount()>0) {
				Thread.yield();
			}
			flushOutputs();
		}
		
		/**
		 * This either casts the argument to an AutoCloseable, or it creates a dummy AutoCloseable.
		 */
		private AutoCloseable getAutoCloseable(Iterator iter) {
			if(iter instanceof AutoCloseable) {
				return (AutoCloseable) iter;
			}
			return new AutoCloseable() {

					@Override
					public void close() {}
			};
		}
		
		/**
		 * Create a new X2Broker.
		 * <p>
		 * First this creates a ModelBroker using the master PrivilegeSet.
		 * Then if this object's constructor included a Dash argument then that is used to create a DashBroker.
		 */
		protected X2Broker createBroker() {
			X2Broker newBroker = new ModelBroker(privilegeSet);
			if(dash!=null)
				newBroker = dash.convertToBrokerDash(newBroker);
			return newBroker;
		}
		
		private void flushOutputs() {
			Object[] outputArray;
			synchronized(outputQueue) {
				outputArray = outputQueue.toArray();
				outputQueue.clear();
			}
			if(outputListener!=null) {
				for(int a = 0; a<outputArray.length; a++) {
					try {
						outputListener.accept( (Output) outputArray[a]);
					} catch(Exception e) {
						handleUncaughtException(e);
					}
				}
			}
		}
		
		/**
		 * This method is notified if either the QueryIteratorFunction or the output Consumer throw an exception
		 * while processing input/output.
		 * <p>
		 * If a Dash has been provided: the default implementation is to try to pass this to the Dash's UncaughtExceptionHandler.
		 * If that is not possible: this prints a SEVERE message to the AppGlobal's log.
		 */
		protected void handleUncaughtException(Exception e) {
			if(dash!=null) {
				UncaughtExceptionHandler ueh = dash.getUncaughtExceptionHandler();
				if(ueh!=null) {
					ueh.uncaughtException(Thread.currentThread(), e);
					return;
				}
			}
			AppGlobals.getLog().log(Level.SEVERE, LoggerUtils.convertThrowableToString(e));
		}
		
		/**
		 * Return the number of milliseconds a consumer thread will wait for new Input before throwing an exception.
		 * The default value of 10 minutes.
		 */
		protected boolean isPollTimeout(long elapsedMillis) {
			return elapsedMillis > 1000*60*10;
		}
		
		/**
		 * Return the number of milliseconds the master thread will wait for a consumer thread to become available.
		 * The default value is 10 minutes.
		 */
		protected boolean isSubmitTimeout(long elapsedMillis) {
			return elapsedMillis > 1000*60*10;
		}
		
		/**
		 * Return the number of active helper threads.
		 * <p>
		 * This is used during shutdown/cleanup to wait for all the consumer threads to exit.
		 */
		protected int getHelperThreadCount() {
			int ctr = 0;
			for(Thread thread : threads) {
				if(thread.isAlive())
					ctr++;
			}
			return ctr;
		}
	}

	/**
	 * This is an OperatorContext that connects Operators to X2BaseBeans.
	 */
	public static final OperatorContext CONTEXT = new OperatorContext() {

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
				throw new RuntimeException("An error occurred retrieving \""
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
	 * This combines an Operator and a bean Class.
	 * It is used as a key to identify TemplateQueryProfiles.
	 */
	public static class ProfileKey extends BasicEntry<Operator, Class<?>> {
		private static final long serialVersionUID = 1L;

		ProfileKey(Operator operator,Class<?> baseClass) {
			super(operator, baseClass);
		}
	}
	
	/**
	 * This combines an Operator, an OrderByComparator, and the isDistinct boolean.
	 * It is used as a key to identify a List of oids.
	 */
	public static class CacheKey extends BasicEntry<Operator, OrderByComparator> {
		private static final long serialVersionUID = 1L;
		
		boolean isDistinct;
		
		CacheKey(Operator operator,OrderByComparator orderBy, boolean isDistinct) {
			super(operator, orderBy);
			this.isDistinct = isDistinct;
		}

		@Override
		public boolean equals(Object obj) {
			if(!super.equals(obj))
				return false;
			CacheKey other = (CacheKey) obj;
			if(isDistinct!=other.isDistinct)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "CacheKey[ \""+getKey()+"\", "+getValue()+(isDistinct ? ", distinct" : "")+"]";
		}
	}
	
	/**
	 * Return a bean from the global cache, or return null if the bean does not
	 * exist in the cache.
	 * 
	 * @param persistenceKey
	 *            the key identifying the database/client.
	 * @param beanClass
	 *            the type of bean to return. This is highly recommended for
	 *            efficiency's sake, but if this is left null it will be derived
	 *            from the bean oid.
	 * @param beanOid
	 *            the oid of the bean to return.
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static X2BaseBean getBeanFromGlobalCache(
			PersistenceKey persistenceKey, Class<?> beanClass, String beanOid) {
		Objects.requireNonNull(persistenceKey);
		Objects.requireNonNull(beanOid);

		X2ObjectCache x2cache = AppGlobals.getCache(persistenceKey);

		if (beanClass == null) {
			beanClass = getBeanTypeFromOid(beanOid);
		}

		Identity identity = new Identity(beanClass, beanClass,
				new Object[] { beanOid });
		X2BaseBean bean = (X2BaseBean) x2cache.lookup(identity);
		return bean;
	}

	public static Class<?> getBeanTypeFromOid(String beanOid) {
		String prefix = beanOid.substring(0, Math.min(3, beanOid.length()));
		BeanTablePath btp = BeanTablePath.getTableByName(prefix
				.toUpperCase());
		if (btp == null)
			throw new IllegalArgumentException(
					"Could not determine bean class for \"" + beanOid
							+ "\".");
		return btp.getBeanType();
	}

	/**
	 * Return all the beans in a list of bean oids, or null if any of those beans were not readily available in the global cache.
	 */
	public List<X2BaseBean> getBeans(PersistenceKey persistenceKey,Class<?> beanType,List<String> beanOids) {
		List<X2BaseBean> beans = new ArrayList<>(beanOids.size());
		for(String beanOid : beanOids) {
			X2BaseBean bean = getBeanByOid(persistenceKey, beanType, beanOid);
			if(bean==null)
				return null;
			beans.add(bean);
		}
		return beans;
	}

	/**
	 * Return true if the argument is a QueryByCriteria whose base class is an X2BaseBean.
	 */
	public static boolean isBeanQuery(Object object) {
		if(!(object instanceof QueryByCriteria))
			return false;
		QueryByCriteria qbc = (QueryByCriteria) object;
		Class baseClass = qbc.getBaseClass();
		return X2BaseBean.class.isAssignableFrom(baseClass);
	}

	/**
	 * This catalogs the number of times certain uncaching operations have been
	 * invoked.
	 */
	public static class CacheResults implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * This describes an attempt to uncache a bean query.
		 */
		public enum Type {
			/**
			 * This indicates caching wasn't attempted for a bean query.
			 */
			QUERY_SKIP, 
			/**
			 * This indicates caching turned up an exact match and no database query was issued.
			 */
			QUERY_HIT, 
			/**
			 * This indicates the Dash caching layer identified the required bean oids, and we replaced the original query with an oid-based query.
			 */
			QUERY_REDUCED_TO_OIDS,
			/**
			 * This indicates we gave up on caching because we had too many beans.
			 */
			QUERY_MISS_ABORT_TOO_MANY,
			/**
			 * This indicates the cache was consulted but didn't have a match.
			 */
			QUERY_MISS,
			/**
			 * This indicates we were able to partially uncache some of the required beans, and we replaced the original query to identify the remaining beans.
			 */
			QUERY_REDUCED_FROM_SPLIT, 
			/**
			 * This indicates our cache layer was able to resolve the query by splitting it and resolving its split elements.
			 */
			QUERY_HIT_FROM_SPLIT,
			/**
			 * This indicates caching wasn't attempted because a Criteria couldn't be converted to an Operator.
			 * (This is probably because a criteria contained a subquery, or some other unsupported feature).
			 */
			QUERY_SKIP_UNSUPPORTED,
			/**
			 * This unusual case indicates that an oid was directly embedded in the query (possibly with other query criteria)
			 */
			QUERY_HIT_FROM_OID,
			/**
			 * This indicates a bean was retrieved from a cache of weakly reference beans. (This is only
			 * attempted when the global app's cache fails.)
			 */
			OID_HIT_REFERENCE,
			/**
			 * This indicates a bean was retrieved from the Aspen's global cache based on its oid.
			 */
			OID_HIT_ASPEN,
			/**
			 * This indicates we tried to look up a bean by its oid but failed.
			 */
			OID_MISS;
		}
		
		//sort keys alphabetically so all CacheResults all follow same pattern
		protected Map<Type, AtomicLong> matches = new TreeMap<>(new Comparator<Type>() {
			@Override
			public int compare(Type o1, Type o2) {
				return o1.name().compareTo(o2.name());
			}
		});

		/**
		 * Increment the counter for a given type of result.
		 * 
		 * @param type the type of result to increment.
		 */
		public void increment(Type type) {
			AtomicLong l;
			synchronized(matches) {
				l = matches.get(type);
				if(l==null) {
					l = new AtomicLong(0);
					matches.put(type, l);
				}
			}
			l.incrementAndGet();
			return;
		}

		@Override
		public int hashCode() {
			synchronized(matches) {
			return matches.hashCode();
			}
		}

		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof CacheResults))
				return false;
			CacheResults other = (CacheResults) obj;
			Map<Type, Long> otherData = other.getData();
			Map<Type, Long> myData = getData();
			return otherData.equals(myData);
		}

		/**
		 * Return a copy of the records in this CacheResults.
		 */
		public Map<Type, Long> getData() {
			Map<Type, Long> returnValue = new HashMap<>();
			synchronized(matches) {
				for(Entry<Type, AtomicLong> entry : matches.entrySet()) {
					returnValue.put(entry.getKey(), entry.getValue().longValue());
				}
			}
			return returnValue;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			for(Entry<Type, AtomicLong> entry : matches.entrySet()) {
				if(sb.length()==0) {
					sb.append("CacheResults[ "+entry.getKey()+"="+entry.getValue());
				} else {
					sb.append(",\n  "+entry.getKey()+"="+entry.getValue());
				}
			}
			if(sb.length()==0)
				sb.append("CacheResults[");
			sb.append("]");
			return sb.toString();
		}

		@SuppressWarnings("unchecked")
		private void readObject(java.io.ObjectInputStream in) throws IOException,
				ClassNotFoundException {
			int version = in.readInt();
			if (version == 0) {
				matches = (Map<Type, AtomicLong>) in.readObject();
			} else {
				throw new IOException("Unsupported internal version: " + version);
			}
		}

		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.writeInt(0);
			out.writeObject(matches);
		}
	}
	
	/**
	 * This monitors a few properties about a template query.
	 * <p>
	 * Here a "template query" means a query stripped of specific values. For example
	 * you query for records where "A==1 || B==true", then the template of that query
	 * resembles "A==? || B==?". Two different queries that use the same fields but
	 * different values match the same template.
	 */
	public static class TemplateQueryProfile implements QueryIteratorDash.CloseListener, Serializable {
		private static final long serialVersionUID = 1L;

		protected int ctr = 0;
		protected int maxReturnCount = 0;
		protected double averageReturnCount = 0;
		protected CacheResults results = new CacheResults();
		
		/**
		 * The total number of times a query matching this template has been issued.
		 */
		public synchronized int getCounter() {
			return ctr;
		}
		
		/**
		 * The average number of beans a QueryIterator iterated over.
		 * <p>
		 * In most cases this is synonymous with "the number of beans a query produces",
		 * but if the iterator is abandoned prematurely then these two values may be
		 * different.
		 */
		public synchronized double getAverageReturnCount() {
			return averageReturnCount;
		}
		
		/**
		 * The maximum number of beans a QueryIterator iterated over.
		 */
		public synchronized int getMaxReturnCount() {
			return maxReturnCount;
		}
		
		/**
		 * Return CacheResults associated with this template.
		 */
		public synchronized CacheResults getResults() {
			return results;
		}

		/**
		 * This object is attached to every QueryIteratorDash a
		 * BrokerDashSharedResource produces, and when that iterator is closed
		 * this object's statistics are updated.
		 */
		@Override
		public synchronized void closedIterator(int returnCount, boolean hasNext) {
			maxReturnCount = Math.max(maxReturnCount, returnCount);
			double total = averageReturnCount * ctr;
			total += returnCount;
			ctr++;
			averageReturnCount = total / ((double)ctr);
		}

		private void readObject(java.io.ObjectInputStream in) throws IOException,
				ClassNotFoundException {
			int version = in.readInt();
			if (version == 0) {
				ctr = in.readInt();
				maxReturnCount = in.readInt();
				averageReturnCount = in.readDouble();
				results = (CacheResults) in.readObject();
			} else {
				throw new IOException("Unsupported internal version: " + version);
			}
		}

		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.writeInt(0);
			out.writeInt(ctr);
			out.writeInt(maxReturnCount);
			out.writeDouble(averageReturnCount);
			out.writeObject(results);
		}

		@Override
		public String toString() {
			return "TemplateQueryProfile[ ctr="+getCounter()+
					", averageReturnCount="+getAverageReturnCount()+
					", maxReturnCount="+getMaxReturnCount()+
					", results="+getResults()+"]";
		}
	}
	
	public static class QueryRequest {
		public final QueryByCriteria beanQuery; 
		public final Operator operator;
		public final TemplateQueryProfile profile;
		public final OrderByComparator orderBy;

		public QueryRequest(QueryByCriteria beanQuery, Operator operator,
				TemplateQueryProfile profile, OrderByComparator orderBy) {
			Objects.requireNonNull(beanQuery);
			Objects.requireNonNull(operator);
			Objects.requireNonNull(profile);
			Objects.requireNonNull(orderBy);
			
			this.beanQuery = beanQuery;
			this.operator = operator;
			this.profile = profile;
			this.orderBy = orderBy;
		}
		
		@Override
		public String toString() {
			return "QueryRequest[ query="+beanQuery+", operator="+operator+", profile="+profile+", orderBy="+orderBy+"]";
		}
		
	}
	
	protected PersistenceKey persistenceKey;
	protected CachePool cachePool;
	protected Cache<ProfileKey, TemplateQueryProfile> profiles;
	protected CacheResults cacheResults = new CacheResults();
	protected Map<Class<?>, Cache<CacheKey, List<String>>> cacheByBeanType = new HashMap<>();
	
	private Logger log = Logger.getAnonymousLogger();
	private ThreadLocal<Logger> logByThread = new ThreadLocal<>();
	
	protected Collection<Class> modifiedBeanTypes = new HashSet<>();
	protected UncaughtExceptionHandler uncaughtExceptionHandler = DEFAULT_UNCAUGHT_EXCEPTION_HANDLER;
	protected WeakReferenceBeanCache weakReferenceCache;
	
	protected boolean isOidCachingActive = true;
	protected boolean isQueryCachingActive = true;
	
	/**
	 * Create a new Dash that keeps up to 5,0000 elements in the cache for up to 5 minutes.
	 */
	public Dash(PersistenceKey persistenceKey) {
		this(persistenceKey, 5000, 1000*60*5);
	}
	
	/**
	 * Create a new Dash.
	 * 
	 * @param maxCacheSize the maximum number of elements that can exist in the cache.
	 * @param maxCacheDuration the maximum duration (in milliseconds) any entry
	 * can exist in the cache.
	 */
	public Dash(PersistenceKey persistenceKey,int maxCacheSize, long maxCacheDuration) {
		this(persistenceKey, new CachePool(maxCacheSize, maxCacheDuration, -1));
	}
	
	/**
	 * Create a new Dash.
	 * 
	 * @param cachePool the CachePool used to maintain all cached data.
	 */
	public Dash(PersistenceKey persistenceKey,CachePool cachePool) {
		Objects.requireNonNull(cachePool);
		Objects.requireNonNull(persistenceKey);
		this.cachePool = cachePool;
		this.persistenceKey = persistenceKey;
		profiles = new Cache<>(cachePool);
		getLog().setLevel(Level.OFF);
		weakReferenceCache = new WeakReferenceBeanCache(this);
	}

	public boolean isOidCachingActive() {
		return isOidCachingActive;
	}

	public boolean isQueryCachingActive() {
		return isQueryCachingActive;
	}

	public boolean setOidCachingActive(boolean b) {
		if(isOidCachingActive==b)
			return false;
		isOidCachingActive = b;
		return true;
	}

	public boolean setQueryCachingActive(boolean b) {
		if(isQueryCachingActive==b)
			return false;
		isQueryCachingActive = b;
		return true;
	}
	
	/**
	 * If {@link #isOidCachingActive()} is true then this attempts
	 * to return the requested X2BaseBean without issuing a query.
	 * <p>
	 * First this consults Aspen's default cache. If that fails then
	 * this consults Dash's local WeakReferenceBeanCache.
	 * 
	 * @param persistenceKey
	 * @param beanClass
	 * @param beanOid
	 * @return
	 */
	public X2BaseBean getBeanByOid(PersistenceKey persistenceKey, Class beanClass,String beanOid) {
		if(isOidCachingActive()==false)
			return null;
		
		Logger log = getLog();
		X2BaseBean bean = getBeanFromGlobalCache(persistenceKey, beanClass, beanOid);
		if(bean!=null) {
			if(log.isLoggable(Level.INFO))
				log.info("global cache resolved "+beanOid);
			cacheResults.increment(CacheResults.Type.OID_HIT_ASPEN);
		} else {
			bean = weakReferenceCache.getBeanByOid(beanClass, beanOid);
			if(bean!=null) {
				if(log.isLoggable(Level.INFO))
					log.info("weak references resolved "+beanOid);
				cacheResults.increment(CacheResults.Type.OID_HIT_REFERENCE);
			} else {
				if(log.isLoggable(Level.INFO))
					log.info("no cache resolved "+beanOid);
				cacheResults.increment(CacheResults.Type.OID_MISS);
			}
		}
		return bean;
	}

	/**
	 * Return the Cache associated with a given bean class.
	 * 
	 * @param beanClass the type of bean to fetch the cache for.
	 * @param createIfMissing if true then may create a new Cache if it doesn't already exist. 
	 * If false then this method may return null.
	 */
	protected Cache<CacheKey, List<String>> getCache(Class<?> beanClass, boolean createIfMissing) {
		synchronized(cacheByBeanType) {
			Cache<CacheKey, List<String>> cache = cacheByBeanType.get(beanClass);
			if(cache==null && createIfMissing) {
				cache = new Cache<>(cachePool);
				cacheByBeanType.put(beanClass, cache);
			}
			return cache;
		}
	}

	/**
	 * Create a QueryIterator for a QueryByCriteria. If {@link #isQueryCachingActive()}
	 * returns false then this immediately lets the broker create the default QueryIterator.
	 * <p>
	 * In an ideal case: this will use cached data to completely avoid making a
	 * database query.
	 * <p>
	 * This method should never issue more than one database query. There are 3
	 * database queries this can issue:
	 * <ul><li>The original incoming query as-is.</li>
	 * <li>A query to retrieve beans based on oids. If this caching layer was able to identify
	 * the exact oids we need, but those beans are no longer in Aspen's cache: a query based
	 * on the oids should be more efficient.</li>
	 * <li>A query to retrieve a subset of the original query. In this case we were able to
	 * split the original query into smaller pieces, and some of those pieces we could uncache
	 * and others we could not.</li></ul>
	 */
	@SuppressWarnings({ "rawtypes" })
	public QueryIterator createQueryIterator(X2Broker broker,QueryByCriteria beanQuery) {
		if(!isQueryCachingActive())
			return broker.getIteratorByQuery(beanQuery);
		
		validatePersistenceKey(broker.getPersistenceKey());
		
		Logger log = getLog();
		if(!isBeanQuery(beanQuery)) {
			if(log.isLoggable(Level.INFO))
				log.info("skipping for non-bean-query: "+beanQuery);
			// the DashInvocationHandler won't even call this method if isBeanQuery(..)==false
			cacheResults.increment(CacheResults.Type.QUERY_SKIP_UNSUPPORTED);
			return broker.getIteratorByQuery(beanQuery);
		}

		Operator operator;
		try {
			operator = createOperator(beanQuery.getCriteria());
		} catch(Exception e) {
			//this Criteria can't be converted to an Operator, so we should give up:

			cacheResults.increment(CacheResults.Type.QUERY_SKIP_UNSUPPORTED);
			return broker.getIteratorByQuery(beanQuery);
		}
		
		Operator template = operator.getTemplateOperator();
		ProfileKey profileKey = new ProfileKey(template, beanQuery.getBaseClass());

		if(log.isLoggable(Level.INFO))
			log.info("template: "+template);
		
		TemplateQueryProfile profile;
		synchronized(profiles) {
			profile = profiles.get(profileKey);
			if(profile==null) {
				profile = new TemplateQueryProfile();
				profiles.put(profileKey, profile);
			}
		}
		if(log.isLoggable(Level.INFO))
			log.info("profile: "+profile);
		
		OrderByComparator orderBy = new OrderByComparator(false,
				beanQuery.getOrderBy());
		
		QueryRequest request = new QueryRequest(beanQuery, operator, profile, orderBy);
		
		Map.Entry<QueryIterator,CacheResults.Type> results = doCreateQueryIterator(broker, request);
		if(log.isLoggable(Level.INFO))
			log.info("produced "+results);
		
		QueryIteratorDash dashIter = results.getKey() instanceof QueryIteratorDash ? (QueryIteratorDash) results.getKey() : null;
		if(dashIter!=null) {
			dashIter.addCloseListener(profile);
			dashIter.addCloseListener(new QueryIteratorDash.CloseListener() {

				@Override
				public void closedIterator(int returnCount, boolean hasNext) {
					Logger log = getLog();
					if(log.isLoggable(Level.INFO))
						log.info("closed iterator after "+returnCount+" iterations, hasNext = "+(hasNext));
				}
				
			});
		} else {
			// if it's not a QueryIteratorDash then our profile/counting mechanism breaks
			if(log.isLoggable(Level.WARNING))
				log.info("produced a QueryIterator that is not a QueryIteratorDash: "+results.getKey().getClass().getName());
		}
		profile.getResults().increment(results.getValue());
		cacheResults.increment(results.getValue());
		return results.getKey();
	}

	/**
	 * Return the overall cache results of all BeanQueries that passed through this object.
	 */
	public CacheResults getCacheResults() {
		return cacheResults;
	}

	/**
	 * Create a QueryIterator for the given query. The current implementation of this
	 * method always returns a QueryIteratorDash, but subclasses can override this
	 * to return something else if needed.
	 * 
	 * @return the iterator and the way to classify this request in CacheResults objects.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map.Entry<QueryIterator,CacheResults.Type> doCreateQueryIterator(X2Broker broker, QueryRequest request) {
		Logger log = getLog();
		if(!isCaching(request)) {
			QueryIterator iter = broker.getIteratorByQuery(request.beanQuery);
			if(log.isLoggable(Level.INFO))
				log.info("aborting to default broker");
			iter = new QueryIteratorDash(this, null, iter);
			return new BasicEntry<>(iter, CacheResults.Type.QUERY_SKIP);
		}
		
		Cache<CacheKey, List<String>> cache = getCache(request.beanQuery.getBaseClass(), true);
		CacheKey cacheKey = new CacheKey(request.operator, request.orderBy, request.beanQuery.isDistinct());
		List<String> beanOids = cache.get(cacheKey);
		
		if (beanOids != null) {
			List<X2BaseBean> beans = getBeans(broker.getPersistenceKey(), request.beanQuery.getBaseClass(), beanOids);
			if(beans!=null) {
				// This is our ideal case: we know the complete query results
				QueryIterator dashIter = new QueryIteratorDash(this, beans);
				if(log.isLoggable(Level.INFO))
					log.info("found "+beans.size()+" beans for "+request);
				return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_HIT);
			}
			
			// We know the exact oids, but those beans aren't in Aspen's cache 
			// anymore. We can at least rewrite the query:
			
			Criteria oidCriteria = new Criteria();
			oidCriteria.addIn(X2BaseBean.COL_OID, beanOids);
			QueryByCriteria newQuery = cloneBeanQuery(request.beanQuery, oidCriteria);

			QueryIterator iter = broker.getIteratorByQuery(newQuery);
			QueryIterator dashIter = new QueryIteratorDash(this, null, iter);

			if(log.isLoggable(Level.INFO))
				log.info("found "+beanOids.size()+" bean oids for "+request);
			
			return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_REDUCED_TO_OIDS);
		}
		
		// we couldn't retrieve the entire query results from our cache

		Operator canonicalOperator = request.operator.getCanonicalOperator();
		Collection<Operator> splitOperators = canonicalOperator.split();
		
		if(isSimpleAttributes(canonicalOperator.getAttributes())) {
			
			Collection<X2BaseBean> beansFromOperator = getBeansFromSplitOperator(broker.getPersistenceKey(), request.beanQuery.getBaseClass(), splitOperators);
			givenSpecificOids : if(beansFromOperator!=null) {
				
				//this is an odd case, but it came up in real-world tests:
				//the query specifically gives us the oid, possibly with other conditions.
				//such as:
				//  contains(oid, {"GRQ00000063MDb", "GRQ00000063M5p"}) && programStudiesOid == "GPR0000001e0Cp"
				//or:
				//  oid == "std01000055744" && studentEvents.eventType == "GRADUATION REQUIREMENT"
				
				// the former should be simple enough to convert using getBeanByOids and evaluate.
				// the latter (because it relies on a related bean) is not safe to optimize here.
				
				Collection<X2BaseBean> returnValue = new TreeSet<>(request.orderBy);
				for(X2BaseBean bean : beansFromOperator) {
					try {
						if(request.operator.evaluate(Dash.CONTEXT, beansFromOperator))
							returnValue.add(bean);
					} catch(Exception e) {
						getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
						break givenSpecificOids;
					}
				}
				QueryIterator dashIter = new QueryIteratorDash(this, returnValue);
				if(log.isLoggable(Level.INFO))
					log.info("filtered "+returnValue.size()+" bean oids for "+request);
				return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_HIT_FROM_OID);
			}
		}

		if(splitOperators.size() <= 1 || !isCachingSplit(request)) {
			// this is the simple scenario (no splitting)
			Collection<X2BaseBean> beansToReturn = new LinkedList<>();
			QueryIterator iter = broker.getIteratorByQuery(request.beanQuery);
			int ctr = 0;
			int maxSize = getMaxOidListSize(false, request.beanQuery);
			while(iter.hasNext() && ctr<maxSize) {
				X2BaseBean bean = (X2BaseBean) iter.next();
				weakReferenceCache.storeBean(bean);
				beansToReturn.add(bean);
				ctr++;
			}
			
			if(iter.hasNext()) {
				// too many beans; let's give up on caching.
				QueryIterator dashIter = new QueryIteratorDash(this, beansToReturn, iter);
				if(log.isLoggable(Level.INFO))
					log.info("query gave up after "+ctr+" iterations for "+request);
				return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_MISS_ABORT_TOO_MANY);
			}
			
			// We have all the beans. Cache the oids for next time and return.

			beanOids = new LinkedList<>();
			for(X2BaseBean bean : beansToReturn) {
				beanOids.add(bean.getOid());
			}
			cache.put(cacheKey, beanOids);
			
			QueryIterator dashIter = new QueryIteratorDash(this, beansToReturn);
			if(log.isLoggable(Level.INFO))
				log.info("queried "+ctr+" iterations for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_MISS);
		}
		
		// We're going to try splitting the operator.
		// (That is: if the original operator was "A==true || B==true", then
		// we may split this into "A" or "B" and look those up/cache those results
		// separately.)
		
		Collection<X2BaseBean> knownBeans;
		if(request.orderBy.getFieldHelpers().isEmpty()) {
			//order doesn't matter
			knownBeans = new LinkedList<>();
		} else {
			knownBeans = new TreeSet<>(request.orderBy);
		}
		
		Iterator<Operator> splitIter = splitOperators.iterator();
		
		int removedOperators = 0;
		while(splitIter.hasNext()) {
			Operator splitOperator = splitIter.next();
			CacheKey splitKey = new CacheKey(splitOperator, request.orderBy, request.beanQuery.isDistinct());
			
			List<String> splitOids = cache.get(splitKey);
			if(splitOids!=null) {
				List<X2BaseBean> splitBeans = 
					getBeans(broker.getPersistenceKey(), 
							request.beanQuery.getBaseClass(), splitOids);
				if(splitBeans!=null) {
					// great: we got *some* of the beans by looking at a split query
					removedOperators++;
					if(log.isLoggable(Level.INFO))
						log.info("resolved split operator "+splitBeans.size()+" beans: "+splitOperator);
					knownBeans.addAll(splitBeans);
					splitIter.remove();
				} else {
					// We know the exact oids, but those beans aren't in Aspen's cache anymore.
					// This splitOperator is a lost cause now: so ignore it.
					cache.remove(splitKey);
					if(log.isLoggable(Level.INFO))
						log.info("identified split operator with "+splitOids.size()+" beans, but purged it: "+splitOperator);
				}
			}
		}
		
		// we removed elements from splitIterators if we resolved those queries, so now
		// all that remains is splitIterators is what we still need to look up.
		
		QueryByCriteria ourQuery = request.beanQuery;
		if(splitOperators.isEmpty()) {
			// we broke the criteria down into small pieces and looked up every piece
			QueryIterator dashIter = new QueryIteratorDash(this, knownBeans);
			if(log.isLoggable(Level.INFO))
				log.info("collection "+knownBeans.size()+" split beans for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_HIT_FROM_SPLIT);
		} else if(removedOperators>0) {
			// we resolved *some* operators, but not all of them.
			Operator trimmedOperator = Operator.join(splitOperators.toArray(new Operator[splitOperators.size()]));
			Criteria trimmedCriteria = createCriteria(trimmedOperator);
			
			// ... so we're going to make a new (narrower) query, and merge its results with knownBeans
			ourQuery = cloneBeanQuery(request.beanQuery, trimmedCriteria);
			if(log.isLoggable(Level.INFO))
				log.info("removed "+ removedOperators+", rewrote as: "+ourQuery);
		}
		
		if(knownBeans.isEmpty()) {
			// No cached info came up so far.
			
			// ... so let's just dump incoming beans in a list. The order is going to be correct,
			// because the order is coming straight from the source. So there's no need
			// to use a TreeSet with a comparator anymore:
			
			knownBeans = new LinkedList<>();
		}

		QueryIterator iter = broker.getIteratorByQuery(ourQuery);
		int ctr = 0;
		int maxSize = getMaxOidListSize(removedOperators>0, ourQuery);
		while(iter.hasNext() && ctr<maxSize) {
			X2BaseBean bean = (X2BaseBean) iter.next();
			weakReferenceCache.storeBean(bean);
			knownBeans.add(bean);
			ctr++;
		}

		if(iter.hasNext()) {
			// too many beans; let's give up on caching.
			QueryIterator dashIter = new QueryIteratorDash(this, knownBeans, iter);
			if(log.isLoggable(Level.INFO))
				log.info("gave up after "+ctr+" iterations for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_MISS_ABORT_TOO_MANY);
		}
		
		// we have all the beans; now we just have to stores things in our cache for next time

		beanOids = new LinkedList<>();
		for(X2BaseBean bean : knownBeans) {
			beanOids.add(bean.getOid());
		}
		cache.put(cacheKey, beanOids);
		
		scanOps : for(Operator op : splitOperators) {
			if(isCachingSplitResults(request, knownBeans)) {
				List<String> oids = new LinkedList<>();
		
				for(X2BaseBean bean : knownBeans) {
					try {
						if(op.evaluate(Dash.CONTEXT, bean)) {
							oids.add(bean.getOid());
						}
					} catch(Exception e) {
						UncaughtExceptionHandler ueh = getUncaughtExceptionHandler();
						Exception e2 = new Exception("An error occurred evaluating \""+op+"\" on \""+bean+"\"", e);
						ueh.uncaughtException(Thread.currentThread(), e2);
						continue scanOps;
					}
				}

				CacheKey splitKey = new CacheKey(op, request.orderBy, ourQuery.isDistinct());
				cache.put(splitKey, oids);
				if(log.isLoggable(Level.INFO))
					log.info("identified "+oids.size()+" oids for split query "+op);
			}
		}

		QueryIterator dashIter = new QueryIteratorDash(this, knownBeans);
		if(removedOperators > 0) {
			if(log.isLoggable(Level.INFO))
				log.info("queried "+ctr+" iterations (with "+removedOperators+" cached split queries) for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_REDUCED_FROM_SPLIT);
		} else {
			if(log.isLoggable(Level.INFO))
				log.info("queried "+ctr+" iterations for "+request);
			return new BasicEntry<>(dashIter, CacheResults.Type.QUERY_MISS);
		}
	}
	
	protected Collection<X2BaseBean> getBeansFromSplitOperator(PersistenceKey persistenceKey, Class beanClass, Collection<Operator> splitOperators) {
		Collection<X2BaseBean> beans = new HashSet<>();
		Logger log = getLog();
		int ctr = 0;
		for(Operator op : splitOperators) {
			
			List ops;
			if(op instanceof And) {
				ops = ((And)op).getOperands();
			} else {
				ops = new ArrayList<>();
				ops.add(op);
			}
			
			EqualTo oidEqualTo = null;
			for(Object z : ops) {
				if(z instanceof EqualTo && X2BaseBean.COL_OID.equals( ((EqualTo)z).getAttribute()) ) {
					oidEqualTo = (EqualTo) z;
					break;
				}
			}
			X2BaseBean bean = oidEqualTo==null ? null :
				getBeanByOid(persistenceKey, beanClass, oidEqualTo.getAttribute() );
			
			if(bean==null) {
				if(log.isLoggable(Level.INFO))
					log.info("aborting after "+ctr+" beans");
				return null;
			}
			
			beans.add(bean);
			ctr++;
		}
		
		if(log.isLoggable(Level.INFO))
			log.info("returning "+beans.size()+" beans");
		
		return beans;
	}

	/**
	 * Return true if the argument doesn't contain a period. For example on the SisStudent bean 
	 * the attribute "nameView" is simple, but "person.firstName" is not simple because it relies 
	 * on a related bean.
	 */
	protected boolean isSimpleAttributes(Collection<String> attributes) {
		for(String attr : attributes) {
			if(attr.indexOf(ModelProperty.PATH_DELIMITER)!=-1)
				return false;
		}
		return true;
	}

	/**
	 * Convert an Operator into an Criteria.
	 */
	public Criteria createCriteria(Operator operator) {
		try {
			return getCriteriaToOperatorConverter().createCriteria(operator);
		} finally {
			Logger log = getLog();
			if(log.isLoggable(Level.INFO))
				log.info(""+operator);
		}
	}
	
	/**
	 * Return the CriteriaToOperatorConverter used to implement
	 * {@link #createCriteria(Operator)} and {@link #createOperator(Criteria)}.
	 */
	protected CriteriaToOperatorConverter getCriteriaToOperatorConverter() {
		return new CriteriaToOperatorConverterImpl();
	}

	/**
	 * Convert a Criteria into an Operator.
	 */
	public Operator createOperator(Criteria criteria) {
		Operator operator = null;
		try {
			operator = getCriteriaToOperatorConverter().createOperator(criteria);
		} finally {
			Logger log = getLog();
			if(log.isLoggable(Level.INFO))
				log.info(""+operator);
		}
		return operator;
	}

	/** 
	 * Create a clone of a bean query with new criteria.
	 */
	protected QueryByCriteria cloneBeanQuery(QueryByCriteria query,Criteria newCriteria) {
		QueryByCriteria returnValue;
		if(query instanceof BeanQuery) {
			BeanQuery b1 = (BeanQuery) query;
			BeanQuery b2 = b1.copy(true);
			b2.setCriteria(newCriteria);
			returnValue = b2;
		} else {
			returnValue = new QueryByCriteria(query.getBaseClass(), newCriteria);
			for(Object orderBy : query.getOrderBy()) {
				FieldHelper fieldHelper = (FieldHelper) orderBy;
				returnValue.addOrderBy(fieldHelper);
			}
		}
		
		return returnValue;
	}

	/**
	 * Return true if we should iterate through all of the beans and cache
	 * exactly which bean oids are associated with the given operator.
	 */
	protected boolean isCachingSplitResults(QueryRequest request, Collection<X2BaseBean> beansToEvaluate) {
		if(!request.orderBy.isSimple())
			return false;
		return true;
	}

	/**
	 * Clear all cached data from memory.
	 * <p>
	 * This is called when {@link X2Broker#clearCache()} is invoked.
	 */
	public void clearAll() {
		try {
			synchronized(cacheByBeanType) {
				cachePool.clear();
				cacheByBeanType.clear();
			}
			weakReferenceCache.clear();
		} finally {
			Logger log = getLog();
			if(log.isLoggable(Level.INFO))
				log.info("");
		}
	}
	
	/**
	 * This method should be notified when the X2Broker saves/updates/deletes
	 * a particular type of bean.
	 * <p>
	 * This clears our cached records for that bean type, and later if
	 * {@link X2Broker#rollbackTransaction()} is called then it will
	 * again clear our cached records for that bean type. (So if you
	 * modify a SisAddress: we have to clear all our address-related cached info.
	 * Then if you rollback your transaction: we need to clear all our
	 * address-related cached info again.)
	 */
	public void modifyBeanRecord(Class beanType) {
		Objects.requireNonNull(beanType);
		synchronized(modifiedBeanTypes) {
			modifiedBeanTypes.add(beanType);
		}
		clearCache(beanType);
		
		Logger log = getLog();
		if(log.isLoggable(Level.INFO))
			log.info(beanType.getName());
	}
	
	public int clearCache(Class beanType) {
		Objects.requireNonNull(beanType);
		int size = -1;
		try {
			Cache<CacheKey, List<String>> cache = getCache(beanType, false);
			if (cache != null) {
				size = cache.size();
				cache.clear();
				return size;
			}
			
			weakReferenceCache.clear(beanType);
			return 0;
		} finally {
			Logger log = getLog();
			if(log.isLoggable(Level.INFO)) {
				if(size==-1) {
					log.info(beanType+", no cache available");
				} else {
					log.info(beanType+", "+size+" entries removed");
				}
			}
		}
	}
	
	/**
	 * Return true if we should consult/update the cache for a given query.
	 */
	protected boolean isCaching(QueryRequest request) {
		Logger log = getLog();
		if(request.profile.getCounter()<10) {
			// The Dash caching layer is supposed to help address
			// frequent repetitive queries. Don't interfere with 
			// rare queries. For large tasks there is usually a huge
			// outermost query/loop (such as grabbing 10,000 students
			// to iterate over). We want to let those big and rare
			// queries slip by this caching model with no interference.

			if(log.isLoggable(Level.INFO))
				log.info("skipping because profile is too small: "+request.profile);
			
			return false;
		}
		
		int max = getMaxOidListSize(false, request.beanQuery);
		if(request.profile.getCounter() > 100 && request.profile.getAverageReturnCount() > max) {
			// If the odds are decent that we're going to get close to
			// our limit: give up now without additional overhead.

			if(log.isLoggable(Level.INFO))
				log.info("skipping because profile shows average exceeds "+max+": "+request.profile);
			
			return false;
		}
		
		return true;
	}
	
	/**
	 * Return the maximum number of oids we'll cache.
	 */
	protected int getMaxOidListSize(boolean involvedSplit,QueryByCriteria query) {
		return 500;
	}
	
	/**
	 * Return true if we should split an Operator to evaluate its elements.
	 */
	protected boolean isCachingSplit(QueryRequest request) {
		if(!request.orderBy.isSimple()) {
			// When you call "myStudent.getPerson().getAddress()", that may
			// involve two separate database queries. So if our order-by comparator
			// involves fetching these properties: that means we may be issuing
			// lots of queries just to sort beans in the expected order.
			// This defeats the purpose of our caching model: if we saved one
			// query but introduced N-many calls to BeanManager#retrieveReference
			// then we may have just made performance (much) worse.
			
			return false;
		}
		
		return true;
	}
	
	static UncaughtExceptionHandler DEFAULT_UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			AppGlobals.getLog().log(Level.SEVERE, "", e);
		}
		
	};
	
	/**
	 * This UncaughtExceptionHandler does nothing; it should be used if you call {@link #setUncaughtExceptionHandler(UncaughtExceptionHandler)} and pass in null.
	 */
	static UncaughtExceptionHandler NULL_UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			//intentionally empty
		}
		
	};
	
	/**
	 * Return the UncaughtExceptionHandler. The default handler writes the stack trace to the AppGlobals log.
	 */
	public UncaughtExceptionHandler getUncaughtExceptionHandler() {
		return uncaughtExceptionHandler;
	}
	
	/**
	 * Assign the UncaughtExceptionHandler.
	 * 
	 * @param ueh the new UncaughtExceptionHandler. If this is null then an empty UncaughtExceptionHandler is used (that does nothing).
	 */
	public void setUncaughtExceptionHandler(UncaughtExceptionHandler ueh) {
		if(ueh==null) ueh = NULL_UNCAUGHT_EXCEPTION_HANDLER;
		uncaughtExceptionHandler = ueh;
	}
	
	/**
	 * Create a BrokerDash that uses this factory's CachePool.
	 * <p>
	 * It is safe to call this method redundantly. If the argument already uses
	 * this factory's ThreadPool then the argument is returned as-is.
	 */
	public BrokerDash convertToBrokerDash(X2Broker broker) {
		return convertToBrokerDash(broker, true);
	}
	
	/**
	 * Create a BrokerDash that uses this factory's CachePool.
	 * <p>
	 * It is safe to call this method redundantly. If the argument already uses
	 * this factory's ThreadPool then the argument is returned as-is.
	 * <p>
	 * @param active this toggles the dash caching logic on/off.
	 */
	public BrokerDash convertToBrokerDash(X2Broker broker, boolean active) {
		validatePersistenceKey(broker.getPersistenceKey());
		
		if (broker instanceof BrokerDash) {
			BrokerDash bd = (BrokerDash) broker;
			Dash sharedResource = bd.getDash();
			if (sharedResource == this) {
				bd.setDashActive(active);
				return bd;
			}
		}
		BrokerDash returnValue = createBrokerDash(broker);
		returnValue.setDashActive(active);
		return returnValue;
	}

	private void validatePersistenceKey(PersistenceKey otherPersistenceKey) {
		if(persistenceKey!=otherPersistenceKey)
			throw new IllegalStateException(persistenceKey+" != "+otherPersistenceKey);
	}

	@SuppressWarnings("rawtypes")
	private BrokerDash createBrokerDash(X2Broker broker) {
		InvocationHandler handler = new DashInvocationHandler(broker, this);

		// include all the old interfaces, in case something else like BrokerCub
		// was already attached
		Class[] oldInterfaces = broker.getClass().getInterfaces();
		Class[] newInterfaces = new Class[oldInterfaces.length + 1];
		System.arraycopy(oldInterfaces, 0, newInterfaces, 0,
				oldInterfaces.length);
		newInterfaces[newInterfaces.length - 1] = BrokerDash.class;

		return (BrokerDash) Proxy.newProxyInstance(
				BrokerDash.class.getClassLoader(), newInterfaces, handler);
	}

	/**
	 * Return the CachePool used by all caches this Dash object maintains.
	 */
	public CachePool getCachePool() {
		return cachePool;
	}
	
	/**
	 * Return a Writer to log debugging information to.
	 */
	public Logger getLog() {
		Logger r = logByThread.get();
		if(r==null)
			r = log;
		return r;
	}
	
	/**
	 * Set a log for debugging.
	 * 
	 * @param appendable the new Appendable to append output to
	 * @param threadSpecific if true then this Appendable will only be consulted
	 * for the current thread. If false then this Appendable will apply to all threads.
	 */
	public void setLog(Logger log,boolean threadSpecific) {
		if(threadSpecific) {
			logByThread.set(log);
		} else {
			logByThread.set(null);
			this.log = log;
		}
	}

	/**
	 * This is called during {@link X2Broker#rollbackTransaction()} to clear
	 * all cached information related beans that may have been changed during
	 * this rollback.
	 */
	public void clearModifiedBeanTypes() {
		Class[] z;
		synchronized(modifiedBeanTypes) {
			z = modifiedBeanTypes.toArray(new Class[modifiedBeanTypes.size()]);
			modifiedBeanTypes.clear();
		}
		
		for(Class c : z) {
			clearCache(c);
		}

		Logger log = getLog();
		if(log.isLoggable(Level.INFO))
			log.info(Arrays.asList(z).toString());
	}

	public PersistenceKey getPersistenceKey() {
		return persistenceKey;
	}
	
	/**
	 * Iterate over a query and process the results separately in additional threads.
	 * 
	 * @param broker the broker that should be used to execute the query (on this thread)
	 * @param query the query to iterate over.
	 * @param privilegeSet a PrivilegeSet used to create additional ModelBrokers.
	 * @param function a function that accepts the query's iterative values and produces output. This function
	 *        will probably be called on helper threads, but if threadCount is 0 then this function will be called
	 *        on this thread. The broker this function receives as an argument will always be a new (unused)
	 *        X2Broker. It is the function's responsibility to set up transactions appropriately.
	 * @param threadCount the number of threads to use to evaluate this query's results. This must be zero or greater.
	 * Each thread can have a unique X2Broker, and each broker can borrow a database connection: so think of this number
	 * as approximately equal to the number of database connections you want to have active at any give time.
	 * If this is zero then the iterator results are processed immediately and no new threads are created.
	 * @param outputConsumer an optional consumer. If this is non-null, then this consumer is given all the Outputs the
	 * function creates. The consumer is only notified on the master thread that originally invoked this method.
	 */
	public <Input, Output> void iterateQueryWithFunction(X2Broker broker,Query query, PrivilegeSet privilegeSet, BrokerIteratorFunction<Input, Output> function, int threadCount, Consumer<Output> outputConsumer) {
		ThreadedIteratorHelper<Input, Output> manager = new ThreadedIteratorHelper<>(privilegeSet, function, threadCount, this, outputConsumer);
		manager.run(broker, query);
	}

	
	/**
	 * Iterate through all elements in an Iterator and process the results separately in additional threads.
	 * 
	 * @param iterator the Iterator to iterate over.
	 * @param privilegeSet a PrivilegeSet used to create additional ModelBrokers.
	 * @param function a function that accepts the query's iterative values and produces output. This function
	 *        will probably be called on helper threads, but if threadCount is 0 then this function will be called
	 *        on this thread. The broker this function receives as an argument will always be a new (unused)
	 *        X2Broker. It is the function's responsibility to set up transactions appropriately.
	 * @param threadCount the number of threads to use to evaluate this query's results. This must be zero or greater.
	 * Each thread can have a unique X2Broker, and each broker can borrow a database connection: so think of this number
	 * as approximately equal to the number of database connections you want to have active at any give time.
	 * If this is zero then the iterator results are processed immediately and no new threads are created.
	 * @param outputConsumer an optional consumer. If this is non-null, then this consumer is given all the Outputs the
	 * function creates. The consumer is only notified on the master thread that originally invoked this method.
	 */
	public <Input, Output> void iterateQueryWithFunction(Iterator iterator, PrivilegeSet privilegeSet, BrokerIteratorFunction<Input, Output> function, int threadCount, Consumer<Output> outputConsumer) {
		ThreadedIteratorHelper<Input, Output> manager = new ThreadedIteratorHelper<>(privilegeSet, function, threadCount, this, outputConsumer);
		manager.run(iterator);
	}
}
