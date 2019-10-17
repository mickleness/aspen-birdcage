package org.abc.tools.procedures;

import org.abc.tools.Tool;

import com.follett.cust.debug.PerformanceMonitorToolSource;

/**
 * This monitors threads on a server and helps diagram where time is
 * being spent.
 */
@Tool(id = "PERF-MONITOR", name = "Performance Monitor", input = "PerformanceMonitorInput.xml", type="export")
public class PerformanceMonitor extends PerformanceMonitorToolSource {
	
	private static final long serialVersionUID = 1L;
	
	// This subclass only exists so the BundleWriter/@Tool annotation
	// will pick this class up and make a .bundle file

}
