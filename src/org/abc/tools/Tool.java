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
}
