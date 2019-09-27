package org.abc.tools;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.apache.ojb.broker.query.Query;

import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.business.ModelBroker;
import com.follett.fsc.core.k12.business.PrivilegeSet;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.x2dev.utils.LoggerUtils;


/**
 * This walks through all the elements in an Iterator and passes them to separate
 * threads for processing. This speeds up the time it takes a tool to execute, but
 * every additional thread requires a unique database connection: so if you give
 * 10 tools 10 threads you're asking for 100 database connections.
 * <p>
 * The thread that sets up and initiates a ThreadedBrokerIterator is considered
 * the master thread. It will create n-many helper threads. 
 * <p>
 * The type of object the iterator produces is the <code>Input</code> for this object.
 * For example: a QueryIterator produced from a BeanQuery for SisStudents will use
 * SisStudents as the Input. Or a column query will use an object array.
 * <p>
 * The Function is called on separate threads to convert an Input to an Output. Each
 * invocation of the function is given a new unique X2Broker.
 * <p>
 * The <code>Output</code> can be anything you want. If you do not need an output: 
 * you can leave this empty and make your function return null. Non-null output
 * object are collected and will be passed back to the optional output Consumer
 * on the master thread. This could useful if you want to save several beans in
 * the master thread's transaction, or if you want to record results using
 * <code>logToolMessage()</code> (which should only be called on the master thread).
 *
 * @param <Input> the object the iterator produces. This object is passed to into
 * the Function on different threads.
 * @param <Output> the output of a Function, which may be passed back to the master 
 * thread for consumption.
 */
public class ThreadedBrokerIterator<Input, Output> {
	
	/**
	 * This is one of the helper threads that listens for inputs and applies the function.
	 * <p>
	 * Helper threads need to be 100% sure they will exit. (We don't want hundreds of orphaned threads
	 * on the server someday.) This thread has three possible conditions under which it will exit:
	 * <ul><li>When completedIterator is toggled from false to true. This is the ideal/typical exit
	 * condition.</li>
	 * <li>When {@link #isPollTimeout(long)} indicates grabbing a new Input off the queue has 
	 * taken too long. This generally shouldn't happen unless something else is frozen/blocked. If all helper 
	 * threads shut down because something just naturally took a VERY long time to complete, then the 
	 * ThreadedBrokerIterator will also eventually shut down when it times out after attempting to pass
	 * Inputs off to the helper threads.</li>
	 * <li>When the tool's master thread is no longer alive. This is considered a fail-safe. This
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
	
	protected PrivilegeSet privilegeSet;
	protected BiFunction<X2Broker, Input, Output> function;
	protected Consumer<Output> outputListener;
	private ArrayBlockingQueue<Input> inputQueue;
	protected int threadCount;
	protected List<Output> outputQueue = new LinkedList<>();
    private AtomicBoolean completedIterator = new AtomicBoolean(false);
    private Thread[] threads;

    /**
     * Create a new ThreadedIteratorHelper.
     * 
	 * @param privilegeSet a PrivilegeSet used to create additional ModelBrokers.
	 * @param function a function that accepts the iterator's values and produces output. This function
	 *        will probably be called on helper threads, but if threadCount is 0 then this function will be called
	 *        on the master thread.
	 * @param threadCount the number of threads to use to evaluate this query's results. This must be zero or greater.
	 * Each additional thread can use a new X2Broker/database connection, so think of this number as the
	 * number of database connections you want to run simultaneously.
	 * <p>
	 * If this is zero then the iterator results are processed on the master thread and no new threads are created.
	 * @param outputConsumer an optional consumer. If this is non-null, then this consumer is given all the Outputs the
	 * function creates. The consumer is only invoked on the master thread.
     */
	public ThreadedBrokerIterator(PrivilegeSet privilegeSet,
			BiFunction<X2Broker, Input, Output> function, 
			int threadCount,Consumer<Output> outputListener) {
		Objects.requireNonNull(privilegeSet);
		Objects.requireNonNull(function);
		if(!(threadCount>=0))
			throw new IllegalArgumentException("threadCount ("+threadCount+") must be at least zero");
		this.privilegeSet = privilegeSet;
		this.function = function;
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
	 * Execute the given query.
	 * 
	 * @param broker the broker used to iterate over the query.
	 * @param query the query to pass to the broker.
	 */
	public void run(X2Broker broker, Query query) {
		Objects.requireNonNull(broker);
		Objects.requireNonNull(query);
		
		QueryIterator iter = broker.getIteratorByQuery(query);
		run(iter);
	}
	
	/**
	 * Iterate over all the elements in an iterator.
	 * 
	 * @param iter the iterator to walk through. If this is an AutoCloseable then it is
	 * automatically closed on completion.
	 */
	public synchronized void run(Iterator iter) {
		//this method is synchronized, so we'll only edit completedIterator on one thread
		completedIterator.set(false);
		
		try(AutoCloseable z = getAutoCloseable(iter)) {
			int elementCtr = 0;
			while(iter.hasNext()) {
				Input value = (Input) iter.next();
				
				if(inputQueue==null) {
					try {
						Output output = function.apply(createBroker(), value);
						if(output!=null)
							outputQueue.add(output);
					} catch(Exception e) {
						handleUncaughtException(e);
					}
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
		} catch(Exception e) {
			handleUncaughtException(e);
		} finally {
			completedIterator.set(true);
		}
		
		// we stopped adding things to our queue, but we need to make sure our helper threads addressed them all:
		while(getActiveHelperThreadCount()>0) {
			try {
				Thread.sleep(10);
			} catch(InterruptedException e) {}
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
	 * Create a new X2Broker for a Thread.
	 */
	protected X2Broker createBroker() {
		X2Broker newBroker = new ModelBroker(privilegeSet);
		return newBroker;
	}
	
	/**
	 * Pass Output objects to the optional output consumer. This method
	 * should only be called on the master thread.
	 */
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
	 * This method is notified when there is an exception.
	 * <p>
	 * The default implementation logs the exception to AppGlobals.
	 */
	protected void handleUncaughtException(Exception e) {
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
	protected int getActiveHelperThreadCount() {
		int ctr = 0;
		for(Thread thread : threads) {
			if(thread.isAlive())
				ctr++;
		}
		return ctr;
	}
}