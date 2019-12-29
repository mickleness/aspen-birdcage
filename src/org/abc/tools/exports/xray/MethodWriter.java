/**
 * This software is released as part of the Pumpernickel project.
 * 
 * All com.pump resources in the Pumpernickel project are distributed under the
 * MIT License:
 * https://raw.githubusercontent.com/mickleness/pumpernickel/master/License.txt
 * 
 * More information about the Pumpernickel project is available here:
 * https://mickleness.github.io/pumpernickel/
 */
package org.abc.tools.exports.xray;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * This writes a java.lang.reflect.Method.
 */
public class MethodWriter extends ConstructorOrMethodWriter {

	/**
	 * Create a new MethodWriter.
	 * 
	 * @param sourceCodeManager
	 *            an optional SourceCodeManager.
	 * @param method
	 *            the method this MethodWriter emulates.
	 */
	public MethodWriter(SourceCodeManager sourceCodeManager, Method method) {
		super(sourceCodeManager, method.getModifiers(), method
				.getTypeParameters(), method.getGenericReturnType(), method
				.getName(), method.getGenericParameterTypes(), method
				.getGenericExceptionTypes(), method.isVarArgs());

		if (method.getDeclaringClass().isInterface()) {
			if (Modifier.isAbstract(modifiers)) {
				modifiers = modifiers - Modifier.ABSTRACT;
				writeBody = false;
			}
		} else if (method.getDeclaringClass().isEnum()) {
			// Don't acknowledge anything inside an enum is abstract.
			// The point of x-ray is to build jars to compile against, and
			// the only time knowing when something is abstract or not will
			// matter to the consumer is when they can subclass it. Nobody
			// can subclass an enum, so it's not worth addressing here.
			if (Modifier.isAbstract(modifiers)) {
				modifiers = modifiers - Modifier.ABSTRACT;
				writeBody = true;
			}
		}
	}
}