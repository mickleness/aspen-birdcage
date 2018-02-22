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
package com.pump.xray;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;

/**
 * This writer is responsible for one Class object (which can be a "class",
 * "interface" or "enum").
 * <p>
 * This class will write a "hollow" or "empty" java source code file that
 * resembles signatures from a java.lang.Class object. (The moniker
 * "mock object" has a special meaning, and it's not really appropriate to call
 * this a mock object.)
 */
public class ClassWriter extends StreamWriter {

	/** The class this writer writes. */
	protected Class type;

	/**
	 * If true a small block of javadoc is added to the topmost declaration in
	 * this file that explains that the origin of the autogenerated code.
	 */
	protected boolean javadocEnabled = true;

	/** The inner classes/interfaces/enums. */
	protected Collection<StreamWriter> members = new TreeSet<>();

	/** The fields to write. */
	protected Collection<FieldWriter> fields = new TreeSet<>();

	/** The constructors to write. */
	protected Collection<ConstructorWriter> constructors = new TreeSet<>();

	/** The methods to write. */
	protected Collection<MethodWriter> methods = new TreeSet<>();

	/**
	 * Create a ClassWriter with no SourceCodeManager.
	 * 
	 * @param type
	 *            the class this ClassWriter will mock/fake.
	 * @param autopopulate
	 *            if true then {@link #autopopulate()} is called to build a
	 *            reasonable set of basic fields/constructors/members.
	 */
	public ClassWriter(Class type, boolean autopopulate) {
		this(null, type, autopopulate);
	}

	/**
	 * Create a ClassWriter.
	 * 
	 * @param sourceCodeManager
	 *            the optional SourceCodeManager that is used to index
	 *            dependencies that this ClassWriter identifies. For example if
	 *            representing the class <code>java.lang.Thread</code> requires
	 *            the method <code>java.util.Map</code>, then that method will
	 *            be added to the sourceCodeManager when this object writes
	 *            data.
	 * @param type
	 *            the class this ClassWriter will mock/fake.
	 * @param autopopulate
	 *            if true then {@link #autopopulate()} is called to build a
	 *            reasonable set of basic fields/constructors/members.
	 */
	public ClassWriter(SourceCodeManager sourceCodeManager, Class type,
			boolean autopopulate) {
		super(sourceCodeManager);
		this.type = type;

		if (autopopulate) {
			autopopulate();
		}
	}

	/**
	 * This grabs basic fields/constructors/methods from the Class this
	 * ClassWriter emulates. Generally this grabs non-private elements, but
	 * there are a few more subtle rules, too:
	 * <ul>
	 * <li>No synthetic fields</li>
	 * <li>If no non-private constructors were found, we include private
	 * constructors</li>
	 * <li>No synthetic methods.</li>
	 * </ul>
	 */
	protected void autopopulate() {
		Field[] fields = type.getDeclaredFields();
		for (Field field : fields) {
			int m = field.getModifiers();
			boolean synthetic = field.isSynthetic();
			if ((!synthetic) && (!Modifier.isPrivate(m)))
				this.fields.add(new FieldWriter(sourceCodeManager, field));
		}
		Constructor[] constructors = type.getDeclaredConstructors();
		for (Constructor constructor : constructors) {
			int m = constructor.getModifiers();
			if (!Modifier.isPrivate(m))
				this.constructors.add(new ConstructorWriter(sourceCodeManager,
						constructor));
		}

		// well... if we couldn't get any public/protected/package level
		// constructors
		// to make visible, then we may HAVE to include private constructors to
		// avoid compiler
		// errors:
		if (this.constructors.size() == 0) {
			for (Constructor constructor : constructors) {
				this.constructors.add(new ConstructorWriter(sourceCodeManager,
						constructor));
			}
		}

		Method[] methods;
		try {
			methods = type.getDeclaredMethods();
		} catch (NoClassDefFoundError e) {
			e.printStackTrace();
			return;
		}
		for (Method method : methods) {
			int m = method.getModifiers();
			boolean include = true;
			if (Modifier.isPrivate(m) || method.getName().startsWith("access$"))
				include = false;
			if (type.isEnum() && method.getName().equals("values")
					&& method.getParameterTypes().length == 0)
				include = false;
			if (type.isEnum() && method.getName().equals("valueOf")
					&& method.getParameterTypes().length == 1
					&& method.getParameterTypes()[0].equals(String.class))
				include = false;
			if (method.isSynthetic())
				include = false;

			if (include)
				this.methods.add(new MethodWriter(sourceCodeManager, method));
		}
	}

	@Override
	public void write(ClassWriterStream cws) throws Exception {
		// I'm not sure what this means, but these have come up a couple of
		// times:
		if (type.getSimpleName().trim().length() == 0)
			return;

		Map<String, String> nameToSimpleName = cws.getNameMap();

		// this is a tricky way of asking if we're the topmost ClassWriter in
		// this source code
		if (nameToSimpleName.size() == 0) {
			nameToSimpleName.clear();
			populateNameMap(nameToSimpleName, this);

			Package p = type.getPackage();
			cws.println("package " + p.getName() + ";");
			cws.println();

			if (javadocEnabled) {
				cws.println(" /**");
				cws.println("  * This class was autogenerated to match the signatures of an existing codebase.");
				cws.println("  * <p>");
				cws.println("  * All methods/constructors/fields in this class are empty. The only exception to this rule is");
				cws.println("  * certain constants (that are Strings and primitives) may be preserved.");
				cws.println("  * <p>");
				cws.println("  * This is intended to create code that you can compile against (as if it were a 3rd party library),");
				cws.println("  * but you can't actually execute anything of interest against this code.");
				cws.println("  * <p>");
				cws.println("  * The tools that created this class are available here:");
				cws.println("  * <br> <a href=\"https://github.com/mickleness/pumpernickel/tree/master/pump-jar/src/main/java/com/pump/xray\">https://github.com/mickleness/pumpernickel/tree/master/pump-jar/src/main/java/com/pump/xray</a>");
				cws.println("  */");
			}
		}

		int modifiers = type.getModifiers();

		if (type.isInterface() && Modifier.isAbstract(modifiers)) {
			// remove things that are implied
			// this is a minor readability nuisance:
			modifiers = modifiers - Modifier.ABSTRACT;
		} else if (type.isEnum()) {
			// these are compiler errors:
			if (Modifier.isFinal(modifiers))
				modifiers = modifiers - Modifier.FINAL;
			if (Modifier.isAbstract(modifiers))
				modifiers = modifiers - Modifier.ABSTRACT;
		}
		cws.print(toString(modifiers));

		if (type.isEnum()) {
			cws.print(" enum ");
			cws.print(type.getSimpleName());
			cws.println(" {");
			try (AutoCloseable c = cws.indent()) {
				writeEnumBody(cws);
			}
			cws.println("}");
		} else {
			if (type.isInterface()) {
				cws.print(" interface ");
			} else {
				cws.print(" class ");
			}
			cws.print(type.getSimpleName());
			TypeVariable[] typeParameters = type.getTypeParameters();
			if (typeParameters.length > 0) {
				cws.print("<");
				for (int a = 0; a < typeParameters.length; a++) {
					if (a > 0)
						cws.print(", ");
					cws.print(toString(nameToSimpleName, typeParameters[a],
							true));
				}
				cws.print(">");
			}

			Type genericSuperclass = type.getGenericSuperclass();
			if (genericSuperclass != null) {
				cws.print(" extends "
						+ toString(nameToSimpleName, genericSuperclass, false));
			}

			Type[] genericInterfaces = type.getGenericInterfaces();

			int classesLen = genericInterfaces.length;
			if (genericInterfaces != null && genericInterfaces.length > 0) {
				if (type.isInterface())
					cws.print(" extends ");
				else
					cws.print(" implements ");
				for (int x = 0; x < classesLen; x++) {
					cws.print(toString(nameToSimpleName, genericInterfaces[x],
							false));
					if (x < classesLen - 1) {
						cws.print(", ");
					}
				}
			}
			cws.println(" {");
			try (AutoCloseable c = cws.indent()) {
				writeClassOrInterfaceBody(cws);
			}
			cws.println("}");
		}
	}

	/**
	 * Populate the nameToSimpleName map.
	 * 
	 * @param nameToSimpleName
	 *            a map of full classnames to simple class names (such as
	 *            "java.lang.Thread" to "Thread")
	 * @param writer
	 *            the writer used to populate the map.
	 */
	private void populateNameMap(Map<String, String> nameToSimpleName,
			ClassWriter writer) {
		String newValue = writer.getType().getSimpleName();
		String oldValue = nameToSimpleName.put(writer.getType().getName()
				.replace('$', '.'), newValue);
		if (oldValue != null && (!oldValue.equals(newValue)))
			throw new IllegalStateException("\"" + oldValue + "\", \""
					+ newValue + "\" " + writer.getType().getName());
		for (StreamWriter member : writer.members) {
			if (member instanceof ClassWriter) {
				populateNameMap(nameToSimpleName, (ClassWriter) member);
			}
		}
	}

	/**
	 * Write a class or interface body (member declarations, fields,
	 * constructors, methods)
	 * 
	 * @param cws
	 *            the stream to write to.
	 */
	protected void writeClassOrInterfaceBody(ClassWriterStream cws)
			throws Exception {
		for (StreamWriter w : members) {
			w.write(cws);
			cws.println();
		}
		for (StreamWriter f : fields) {
			f.write(cws);
		}
		for (ConstructorWriter c : constructors) {
			cws.println();
			c.write(cws);
		}
		for (MethodWriter m : methods) {
			cws.println();
			m.write(cws);
		}
	}

	/**
	 * Write an enum body (enum constants, fields, methods). Constructors are
	 * omitted, because 3rd parties will never need to construct an enum.
	 * 
	 * @param cws
	 *            the stream to write to.
	 */
	protected void writeEnumBody(ClassWriterStream cws) throws Exception {
		for (StreamWriter w : members) {
			w.write(cws);
			cws.println();
		}
		boolean started = false;
		for (FieldWriter f : fields) {
			if (f.getField().isEnumConstant()) {
				if (started)
					cws.print(", ");
				cws.print(f.getField().getName());
				started = true;
			}
		}
		if (started)
			cws.println(";");

		for (FieldWriter f : fields) {
			if (!f.getField().isEnumConstant()) {
				f.write(cws);
			}
		}
		for (MethodWriter m : methods) {
			cws.println();
			m.write(cws);
		}
	}

	/** Return the class this ClassWriter writes. */
	public Class getType() {
		return type;
	}

	/** Add a declared/nested class inside this class. */
	public void addDeclaredClass(ClassWriter writer) {
		members.add(writer);
	}

	/**
	 * Return the ClassWriter associated with a declared (member/nested) type,
	 * or null if no ClassWriter is identified.
	 */
	public ClassWriter getDeclaredType(Class declaredType) {
		for (StreamWriter member : members) {
			if (member instanceof ClassWriter
					&& ((ClassWriter) member).getType().equals(declaredType)) {
				return ((ClassWriter) member);
			}
		}
		return null;
	}

	/**
	 * Return true if the javadoc-enabled flag is active. If this is true then
	 * the topmost declaration in this ClassWriter will receive a small javadoc
	 * explanation of how this code was autogenerated.
	 */
	public boolean isJavadocEnabled() {
		return javadocEnabled;
	}

	/** Set the javadoc-enabled flag. */
	public void setJavadocEnabled(boolean b) {
		javadocEnabled = b;
	}

}