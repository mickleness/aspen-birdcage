package org.abc.tools;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
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
import com.x2dev.utils.ThreadUtils;

/**
 * This walks through all the elements in an Iterator and passes them to
 * separate threads for processing. This speeds up the time it takes a tool to
 * execute, but every additional thread requires a unique database connection:
 * so if you give 10 tools 10 threads you're asking for 100 database
 * connections.
 * <p>
 * The thread that sets up and initiates a ThreadedBrokerIterator is considered
 * the master thread. It will create n-many helper threads.
 * <p>
 * The type of object the iterator produces is the <code>Input</code> for this
 * object. For example: a QueryIterator produced from a BeanQuery for
 * SisStudents will use SisStudents as the Input. Or a column query will use an
 * object array.
 * <p>
 * The BiFunction is usually called on helper threads to convert an Input to an
 * Output. Each invocation of the function is given a new unique X2Broker.
 * <p>
 * When all the helper threads are busy the master thread also processes
 * elements from the queue, so the master thread can occasionally invoke the
 * function, too.
 * <p>
 * The <code>Output</code> can be anything you want. If you do not need an
 * output: you can leave this empty and make your function return null. Non-null
 * output object are collected and will be passed back to the optional output
 * Consumer on the master thread. This could useful if you want to save several
 * beans in the master thread's transaction, or if you want to record results
 * using <code>logToolMessage()</code> (which should only be called on the
 * master thread).
 *
 * @param <Input>
 *            the object the iterator produces. This object is passed to into
 *            the Function on different threads.
 * @param <Output>
 *            the output of a Function, which may be passed back to the master
 *            thread for consumption.
 */
public class ThreadedBrokerIterator<Input, Output> {

	public static class ThreadedException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		Map<Exception, Object> exceptions;

		/**
		 * @param exceptions
		 *            a map of exceptions to the iterator value that triggered
		 *            them.
		 */
		public ThreadedException(Map<Exception, Object> exceptions) {
			this.exceptions = Collections.unmodifiableMap(exceptions);
			// let the natural caused-by chain be as informative as possible
			if (exceptions.size() > 0)
				initCause(exceptions.keySet().iterator().next());
		}

		/**
		 * Return a map of exceptions to the iterator value that triggered them.
		 */
		public Map<Exception, Object> getExceptions() {
			return exceptions;
		}
	}

    /**
     * This throws a CancellationException just like {@link ThreadUtils#checkInterrupt()},
     * but this does NOT call Thread.yield.
     */
    public static void checkInterruptNoYield() {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Stopped by interrupt check");
        }
    }

	/**
	 * This is one of the helper threads that listens for inputs and applies the
	 * function.
	 * <p>
	 * Helper threads need to be 100% sure they will exit. (We don't want
	 * hundreds of orphaned threads on the server someday.) This thread has
	 * three possible conditions under which it will exit:
	 * <ul>
	 * <li>When completedIterator is toggled from false to true. This is the
	 * ideal/typical exit condition.</li>
	 * <li>When {@link #isPollTimeout(long)} indicates grabbing a new Input off
	 * the queue has taken too long. This generally shouldn't happen unless
	 * something else is frozen/blocked. If all helper threads shut down because
	 * something just naturally took a VERY long time to complete, then the
	 * ThreadedBrokerIterator will also eventually shut down when it times out
	 * after attempting to pass Inputs off to the helper threads.</li>
	 * <li>When the tool's master thread is no longer alive. This is considered
	 * a fail-safe. This should never be needed, because the completedIterator
	 * boolean should be safely changed inside a finally block.</li>
	 * </ul>
	 * <p>
	 */
	class HelperThread extends Thread {
		Thread masterThread;
		AtomicBoolean completedIterator;

		HelperThread(int index,AtomicBoolean completedIterator) {
			super(Thread.currentThread().getName() + "-helper-" + index);
			masterThread = Thread.currentThread();
			this.completedIterator = completedIterator;
		}

		@Override
		public void run() {
			long lastInput = System.currentTimeMillis();
			boolean keepRunning = true;
			boolean inputQueueStoppedAcceptingAdditions = false;
			while (keepRunning) {
				if (!masterThread.isAlive()) {
					keepRunning = false;
					return;
				}

				synchronized (exceptions) {
					// the master thread should throw an exception, we should
					// just abort
					if (!exceptions.isEmpty())
						return;
				}

				// I originally tried calls like inputQueue.poll(50,
				// TimeUnit.MILLIS), but for some reason
				// when I used those calls and killed the master tool: this
				// helper thread seemed to get locked and never recover. I 
				// didn't figure out exactly why, but switching to this
				// poll() method appeared to resolve it:
				Input input = inputQueue.poll();

				try {
					if (input != null) {
						lastInput = System.currentTimeMillis();
						Output output = function.apply(createBroker(), input);
						if (output != null) {
							synchronized (outputQueue) {
								outputQueue.add(output);
							}
						}
					} else {
						if (inputQueueStoppedAcceptingAdditions) {
							// First we noticed completedIterator is true, then
							// we drained the inputQueue. We can end this thread
							// now.
							return;
						}
						
						long elapsed = System.currentTimeMillis() - lastInput;
						if (isPollTimeout(elapsed)) {
							keepRunning = false;
							throw new RuntimeException(
									"Unexpectedly slow input queue: " + elapsed
											+ " ms");
						}
					}
				} catch (Exception e) {
					if (handleUncaughtException(e)) {
						synchronized (exceptions) {
							exceptions.put(e, input);
						}
					}
				}

				if (completedIterator.get()) {
					inputQueueStoppedAcceptingAdditions = true;
				}
			}
		}
	}

	/**
	 * Maps an Exception to the input value that triggered it.
	 * <p>
	 * When this is non-empty, the master thread should throw an exception.
	 * Calls to this map should be synchronized against this map.
	 */
	protected Map<Exception, Object> exceptions = new LinkedHashMap<>();
	protected PrivilegeSet privilegeSet;
	protected BiFunction<X2Broker, Input, Output> function;
	protected Consumer<Output> outputListener;
	private ArrayBlockingQueue<Input> inputQueue;
	protected List<Output> outputQueue = new LinkedList<>();
	int threadCount;

	/**
	 * Create a new ThreadedIteratorHelper.
	 * 
	 * @param privilegeSet
	 *            a PrivilegeSet used to create additional ModelBrokers.
	 * @param function
	 *            a function that accepts the iterator's values and produces
	 *            output. This function will probably be called on helper
	 *            threads, but if threadCount is 0 then this function will be
	 *            called on the master thread.
	 * @param threadCount
	 *            the total number of threads, including the current thread, to
	 *            use to evaluate the query's results. For example: if this is
	 *            10, then 9 additional threads will be created. This must be
	 *            one or greater. This is also the number of additional
	 *            X2Brokers that may be created at any given time, so you can
	 *            think of this number as the number of additional database
	 *            connections this object will use.
	 *            <p>
	 *            If this is one then the iterator results are processed on the
	 *            master thread and no new threads are created.
	 * @param outputConsumer
	 *            an optional consumer. If this is non-null, then this consumer
	 *            is given all the Outputs the function creates. The consumer is
	 *            only invoked on the master thread.
	 */
	public ThreadedBrokerIterator(PrivilegeSet privilegeSet,
			BiFunction<X2Broker, Input, Output> function, int threadCount,
			Consumer<Output> outputListener) {
		Objects.requireNonNull(privilegeSet);
		Objects.requireNonNull(function);
		if (!(threadCount >= 1))
			throw new IllegalArgumentException("threadCount (" + threadCount
					+ ") must be at least one");
		this.privilegeSet = privilegeSet;
		this.function = function;
		this.threadCount = threadCount;
		this.outputListener = outputListener;

		// Our master thread (that puts things on the queue) will automatically
		// switch to become a worker thread if the queue reaches its capacity.
		// Therefore we want the queue to be 3x the number of threads:
		// if the primary thread switches to a worker thread we want there to be
		// plenty (at *least* 2x) elements for other worker threads to work on
		// before the primary thread resumes building up the queue again
		int queueCapacity = Math.max(10, threadCount * 3);

		// ... this assumes each invocation of the function is similar in
		// weight. If the master thread picks up a task that's 10x more work
		// than most other tasks: this model still falls apart.

		inputQueue = new ArrayBlockingQueue<Input>(queueCapacity);
	}

	/**
	 * Execute the given query.
	 * 
	 * @param broker
	 *            the broker used to iterate over the query.
	 * @param query
	 *            the query to pass to the broker.
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
	 * @param iter
	 *            the iterator to walk through. If this is an AutoCloseable then
	 *            it is automatically closed on completion.
	 */
	public void run(Iterator iter) {
		final AtomicBoolean completedIterator = new AtomicBoolean(false);
		
		//this thread may also be a worker thread, so use "threadCount - 1"
		Thread[] threads = new Thread[threadCount - 1];
		for (int a = 0; a < threads.length; a++) {
			threads[a] = new HelperThread(a, completedIterator);
			threads[a].start();
		}
		
		try {
			// this method is synchronized, so we'll only edit completedIterator on
			// one thread
			completedIterator.set(false);

			try (AutoCloseable z = getAutoCloseable(iter)) {
				while (iter.hasNext()) {
					checkInterruptNoYield();

					synchronized (exceptions) {
						if (!exceptions.isEmpty()) {
							throw new ThreadedException(exceptions);
						}
					}

					Input value = (Input) iter.next();

					if (!inputQueue.offer(value)) {
						// If all our other threads are busy: then we become a
						// worker thread. We made sure the inputQueue was large 
						// enough that the other threads should have something 
						// to work on if they finish their task
						runFunctionOnMasterThread(value);
					}

					flushOutputs();
				}
			} catch (CancellationException e) {
				inputQueue.clear();
				throw e;
			} catch (ThreadedException e) {
				inputQueue.clear();
				throw e;
			} catch (Exception e) {
				// this would be an extremely rare case
				if (handleUncaughtException(e)) {
					synchronized (exceptions) {
						exceptions.put(e, null);
					}
					throw new ThreadedException(exceptions);
				}
			} finally {
				completedIterator.set(true);
			}

			// we stopped adding things to our queue, but we need to make sure our
			// helper threads addressed them all:
			while (getActiveHelperThreadCount(threads) > 0) {
				checkInterruptNoYield();

				synchronized (exceptions) {
					if (!exceptions.isEmpty()) {
						throw new ThreadedException(exceptions);
					}
				}

				Input input = inputQueue.poll();
				if (input != null) {
					runFunctionOnMasterThread(input);
				} else {
					// out queue is empty, but helper threads are still wrapping up.
					// So we wait:
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
					}
				}
			}

			flushOutputs();

			synchronized (exceptions) {
				if (!exceptions.isEmpty()) {
					throw new ThreadedException(exceptions);
				}
			}
		} finally {
			// We should be all done by the time we reach here.
			
			// Once (in over 100 trials) I saw a helper thread on the app server
			// long after the master thread was canceled just sitting there with
			// this stack trace:
			// Current stack trace for tool-job-1369309-helper-6
			// com.microsoft.sqlserver.jdbc.TDSCommand.close(IOBuffer.java:5781)
			// com.microsoft.sqlserver.jdbc.SQLServerStatement.discardLastExecutionResults(SQLServerStatement.java:94)
			// com.microsoft.sqlserver.jdbc.SQLServerStatement.closeInternal(SQLServerStatement.java:584)
			// com.microsoft.sqlserver.jdbc.SQLServerStatement.close(SQLServerStatement.java:596)
			// com.follett.fsc.core.framework.persistence.ConnectionFactoryX2Impl$ConPoolFactory.validateConnection(ConnectionFactoryX2Impl.java:740)
			// com.follett.fsc.core.framework.persistence.ConnectionFactoryX2Impl$ConPoolFactory.validateObject(ConnectionFactoryX2Impl.java:689)
			// org.apache.commons.pool.impl.GenericObjectPool.borrowObject(GenericObjectPool.java:788)
			// com.follett.fsc.core.framework.persistence.ConnectionFactoryX2Impl.getConnectionFromPool(ConnectionFactoryX2Impl.java:368)
			// org.apache.ojb.broker.accesslayer.ConnectionFactoryAbstractImpl.lookupConnection(ConnectionFactoryAbstractImpl.java:116)
			// org.apache.ojb.broker.accesslayer.ConnectionManagerImpl.getConnection(ConnectionManagerImpl.java:105)
			// org.apache.ojb.broker.accesslayer.ConnectionManagerImpl.localBegin(ConnectionManagerImpl.java:147)
			// org.apache.ojb.broker.core.PersistenceBrokerImpl.beginTransaction(PersistenceBrokerImpl.java:402)
			// org.apache.ojb.broker.core.DelegatingPersistenceBroker.beginTransaction(DelegatingPersistenceBroker.java:139)
			// org.apache.ojb.broker.core.DelegatingPersistenceBroker.beginTransaction(DelegatingPersistenceBroker.java:139)
			// com.follett.fsc.core.k12.beans.BeanManager.beginTransaction(BeanManager.java:1276)

			// So knowing that broker threads can randomly hang, let's force all
			// our threads to either finish or die. (This is analogous to what
			// the ToolJob does to the master thread.)
			
			try {
				boolean wait = false;
				for (Thread thread : threads) {
					if(thread.isAlive()) {
						thread.interrupt();
						wait = true;
					}
				}
				
				if(wait) {
					Thread.sleep(500);
				}
			} catch(InterruptedException e) {
				//intentionally empty
			} finally {
				for (Thread thread : threads) {
					if(thread.isAlive()) {
						thread.stop();
					}
				}
			}
		}
	}

	private void runFunctionOnMasterThread(Input value) {
		try {
			Output output = function.apply(createBroker(), value);
			if (output != null) {
				synchronized (outputQueue) {
					outputQueue.add(output);
				}
			}
		} catch (Exception e) {
			if (handleUncaughtException(e)) {
				synchronized (exceptions) {
					exceptions.put(e, value);
				}
			}
		}
	}

	/**
	 * This either casts the argument to an AutoCloseable, or it creates a dummy
	 * AutoCloseable.
	 */
	private AutoCloseable getAutoCloseable(Iterator iter) {
		if (iter instanceof AutoCloseable) {
			return (AutoCloseable) iter;
		}
		return new AutoCloseable() {

			@Override
			public void close() {
			}
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
	 * Pass Output objects to the optional output consumer. This method should
	 * only be called on the master thread.
	 */
	private void flushOutputs() {
		Object[] outputArray;
		synchronized (outputQueue) {
			outputArray = outputQueue.toArray();
			outputQueue.clear();
		}
		if (outputListener != null) {
			for (int a = 0; a < outputArray.length; a++) {
				try {
					outputListener.accept((Output) outputArray[a]);
				} catch (Exception e) {
					handleUncaughtException(e);
				}
			}
		}
	}

	/**
	 * This method is notified when there is an exception.
	 * <p>
	 * The default implementation logs the exception to AppGlobals.
	 * 
	 * @return true if this exception should cause all threads to abort and stop
	 *         polling more inputs. In this case all threads should finish their
	 *         current tasks, but not pick up any more tasks. The master thread
	 *         should throw a ThreadedException.
	 *         <p>
	 *         If this returns false: all threads continue as usual.
	 *         <p>
	 *         The default implementation returns true.
	 */
	protected boolean handleUncaughtException(Exception e) {
		AppGlobals.getLog().log(Level.SEVERE,
				LoggerUtils.convertThrowableToString(e));
		return true;
	}

	/**
	 * Return the number of milliseconds a consumer thread will wait for new
	 * Input before throwing an exception. The default value of 10 minutes.
	 */
	protected boolean isPollTimeout(long elapsedMillis) {
		return elapsedMillis > 1000 * 60 * 10;
	}

	/**
	 * Return the number of active helper threads.
	 * <p>
	 * This is used during shutdown/cleanup to wait for all the consumer threads
	 * to exit.
	 */
	protected int getActiveHelperThreadCount(Thread[] threads) {
		int ctr = 0;
		for (Thread thread : threads) {
			if (thread.isAlive())
				ctr++;
		}
		return ctr;
	}
}