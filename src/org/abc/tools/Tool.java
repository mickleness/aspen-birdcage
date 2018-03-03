package org.abc.tools;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation helps the {@link BundleWriter} automatically generate tool
 * bundles.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Tool {
	/** The ID of this tool. */
	String id();

	/** The name of this tool. */
	String name();

	/**
	 * This optional attribute should be "import", "export", "procedure",
	 * "report". If it is missing, then we'll try to guess the tool type based
	 * on what the tool class extends.
	 */
	String type() default "";

	/**
	 * This optional attribute identifies the filename of the XML file
	 * containing the input parameters.
	 */
	String input() default "";

	/**
	 * This optional attribute identifies the category of the tool.
	 */
	String category() default "";

	/**
	 * This optional attribute returns an array of the nodes to write in the
	 * tool bundle. For example one String this may return might be:
	 * 
	 * <pre>
	 * key="student.std.list" build-view="true" org1-view="true" health-view="true" iep-view="true" school-view="true" staff-view="true"
	 * </pre>
	 */
	String[] nodes() default "";
}
