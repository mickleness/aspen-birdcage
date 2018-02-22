package com.pump.io;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This monitors a <code>java.io.File</code> directory and notifies
 * its listeners when files are added, removed or changed.
 */
public class DirectoryMonitor implements AutoCloseable
{

	/**
	 * This listener is notified every time there new, deleted, or modified files
	 * to report.
	 * <p>
	 * Note that this is not guaranteed to capture all events. For example,
	 * if a file is created and deleted before this monitor polls the file system:
	 * it won't trigger any events.
	 */
    public interface Listener
    {
    	public void listingChanged(File[] filesAdded,File[] filesDeleted,File[] filesChanged);
    }

    protected final File dir;
    protected final long pollingInterval;
    
    protected List<Listener> listeners = new ArrayList<>();
    protected ScheduledExecutorService service;
    protected ScheduledFuture scheduledFuture;
    
    /**
     * This Runnable is static and uses a WeakReference to make sure it doesn't
     * prevent the DirectoryMonitor from being garbage collected as necessary.
     */
    static class ProcessFileRunnable implements Runnable {
    	Map<File, Long> fileModificationMap = new HashMap<>();
    	WeakReference<DirectoryMonitor> directoryMonitorRef;
    	
    	ProcessFileRunnable(DirectoryMonitor monitor) {
    		if(monitor==null)
    			throw new NullPointerException();
    		directoryMonitorRef = new WeakReference<>(monitor);
    	}
    	
    	@SuppressWarnings("resource")
		public void run() {
    		DirectoryMonitor monitor = directoryMonitorRef.get();
    		if(monitor==null)
    			return;
    		
    		File[] children = monitor.getFile().listFiles();
    		
    		List<File> added = new LinkedList<>();
    		List<File> changed = new LinkedList<>();
    		List<File> removed = new LinkedList<>();
    		
    		Collection<File> processed = new HashSet<>();
    		for(File child : children) {
    			processed.add(child);
    			Long recordedLastModifiedTime = fileModificationMap.get(child);
    			long liveLastModifiedTime = child.lastModified();
    			if(recordedLastModifiedTime==null) {
    				added.add(child);
    				fileModificationMap.put(child, liveLastModifiedTime);
    			} else if(recordedLastModifiedTime.longValue()!=liveLastModifiedTime) {
    				changed.add(child);
    				fileModificationMap.put(child, liveLastModifiedTime);
    			}
    		}
    		Iterator<File> iter = fileModificationMap.keySet().iterator();
    		while(iter.hasNext()) {
    			File file = iter.next();
    			if(!processed.contains(file)) {
    				removed.add(file);
    				iter.remove();
    			}
    		}
    		
    		if(added.size()>0 || changed.size()>0 || removed.size()>0) {
    			File[] addedArray = added.toArray(new File[added.size()]);
    			File[] changedArray = changed.toArray(new File[changed.size()]);
    			File[] removedArray = removed.toArray(new File[removed.size()]);
    			Listener[] listenerArray;
    			synchronized(monitor) {
    				listenerArray = monitor.listeners.toArray(new Listener[monitor.listeners.size()]);
    			}
    			for(Listener l : listenerArray) {
    				try {
    					l.listingChanged(addedArray, removedArray, changedArray);
    				} catch(Exception e) {
    					monitor.handleListenerException(e);
    				}
    			}
    		}
    	}
    }
	
    /**
     * Create a new DirectoryMonitor;
     * 
     * @param file the directory to monitor. This must already exist
     * or this method throws an Exception.
     * @param pollingInterval the number of milliseconds between polls.
     */
	public DirectoryMonitor(File file,long pollingInterval) {
		if(file==null)
			throw new NullPointerException();
		if(!file.exists())
			throw new IllegalArgumentException();
		if(!file.isDirectory())
			throw new IllegalArgumentException();
		dir = file;
		this.pollingInterval = pollingInterval;
		
		service = Executors.newSingleThreadScheduledExecutor();
	}
	
	/**
	 * This method is called when a Listener throws an exception.
	 * <p>
	 * This method is provided as a convenience for subclasses to override. The
	 * default implementation just calls <code>e.printStackTrace()</code>.
	 */
	protected void handleListenerException(Exception e)
	{
		e.printStackTrace();
	}
	
	public synchronized void addListener(Listener listener) {
		if(listener==null)
			throw new NullPointerException();
		listeners.add(listener);
		if(listeners.size()==1) {
			scheduledFuture = service.scheduleAtFixedRate( new ProcessFileRunnable(this), 0, pollingInterval, TimeUnit.MILLISECONDS);
		}
	}
	
	public synchronized void removeListener(Listener listener) {
		if(listeners.remove(listener)) {
    		if(listeners.size()==0 && scheduledFuture!=null) {
    			scheduledFuture.cancel(false);
    		}
		}
	}

	/**
	 * Return the directory we're monitoring.
	 */
	public File getFile()
	{
		return dir;
	}
	
	@Override
	protected void finalize() throws Exception {
		close();
	}

	/**
	 * This terminates the polling thread (executor) so we stop listening to the directory.
	 * Once this method is called this object cannot be reactivated.
	 */
	@Override
	public synchronized void close() throws Exception
	{
		scheduledFuture.cancel(false);
		service.shutdown();
		service = null;
	}
}
