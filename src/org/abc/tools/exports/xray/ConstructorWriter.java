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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * This writes a java.lang.reflect.Constructor.
 */
public class ConstructorWriter extends ConstructorOrMethodWriter {

	Constructor constructor;

	/**
	 * Create a new ConstructorWriter.
	 * 
	 * @param sourceCodeManager
	 *            an optional SourceCodeManager.
	 * @param constructor
	 *            the Constructor this writer represents.
	 */
	public ConstructorWriter(SourceCodeManager sourceCodeManager,
			Constructor constructor) {
		super(sourceCodeManager, constructor.getModifiers(), constructor
				.getTypeParameters(), null, constructor.getDeclaringClass()
				.getSimpleName(), constructor.getGenericParameterTypes(),
				constructor.getGenericExceptionTypes(), constructor.isVarArgs());
		this.constructor = constructor;

		boolean isStaticClass = (constructor.getDeclaringClass().getModifiers() & Modifier.STATIC) > 0;
		// for inner classes where the first parameter is the declaring class,
		// trim that
		if (!isStaticClass
				&& isNestedClassWithArtificialFirstArgument(constructor)) {
			paramTypes = removeFirstElement(Type.class, paramTypes);
		}
	}

	private static <T> T[] removeFirstElement(Class<T> arrayElementType,
			T[] original) {
		T[] copy = (T[]) Array.newInstance(arrayElementType,
				original.length - 1);
		System.arraycopy(original, 1, copy, 0, copy.length);
		return copy;
	}

	private static boolean isNestedClassWithArtificialFirstArgument(
			Constructor c) {
		if (c.getDeclaringClass().getDeclaringClass() != null) {
			Class[] params = c.getParameterTypes();
			if (params.length > 0
					&& params[0].equals(c.getDeclaringClass()
							.getDeclaringClass())) {
				return true;
			}
		}

		return false;
	}

	/**
	 * This picks an appropriate "super(..)" constructor to write. It tries to
	 * find any constructor that appropriately matches the throws declaration of
	 * this constructor. This also takes into accounts generics, which can get a
	 * little tricky.
	 */
	@Override
	protected void writeBody(ClassWriterStream cws) {
		Class type = constructor.getDeclaringClass();
		type = type.getSuperclass();
		if (type != null) {
			Constructor[] superConstructors = type.getDeclaredConstructors();
			Comparator<Constructor> constructorComparator = new Comparator<Constructor>() {

				@Override
				public int compare(Constructor o1, Constructor o2) {
					Class[] throws1 = o1.getExceptionTypes();
					Class[] throws2 = o2.getExceptionTypes();
					boolean compatibleThrows1 = isCompatibleThrows(throws1);
					boolean compatibleThrows2 = isCompatibleThrows(throws2);
					if ((compatibleThrows1) && (!compatibleThrows2))
						return -1;
					if ((!compatibleThrows1) && (compatibleThrows2))
						return 1;

					boolean public1 = Modifier.isPublic(o1.getModifiers());
					boolean public2 = Modifier.isPublic(o2.getModifiers());
					if ((public1) && (!public2))
						return -1;
					if ((!public1) && (public2))
						return 1;

					boolean protected1 = Modifier
							.isProtected(o1.getModifiers());
					boolean protected2 = Modifier
							.isProtected(o2.getModifiers());
					if ((protected1) && (!protected2))
						return -1;
					if ((!protected1) && (protected2))
						return 1;

					boolean private1 = Modifier.isPrivate(o1.getModifiers());
					boolean private2 = Modifier.isPrivate(o2.getModifiers());
					if ((private1) && (!private2))
						return 1;
					if ((!private1) && (private2))
						return -1;

					return o1.toString().compareTo(o2.toString());
				}

				private boolean isCompatibleThrows(Class[] throwTypes) {
					for (Class throwType : throwTypes) {
						if (!isCompatible(throwType))
							return false;
					}
					return true;
				}

				private boolean isCompatible(Class throwType) {
					for (Type t : ConstructorWriter.this.throwsTypes) {
						Class c = null;
						if (t instanceof Class) {
							c = (Class) t;
						}
						if (c != null && c.isAssignableFrom(throwType))
							return true;
					}

					return false;
				}

			};

			Arrays.sort(superConstructors, constructorComparator);
			cws.print("super(");

			// This is a map of type variable names, relative to the super
			// class.
			Map<String, Type> typeVariableNameMap = new HashMap<>();
			Type[] genericParams = superConstructors[0]
					.getGenericParameterTypes();
			TypeVariable[] superTypeParameters = superConstructors[0]
					.getDeclaringClass().getTypeParameters();
			Type declaringSuperClassGenerics = constructor.getDeclaringClass()
					.getGenericSuperclass();
			if (declaringSuperClassGenerics instanceof ParameterizedType) {
				ParameterizedType pt = (ParameterizedType) declaringSuperClassGenerics;
				Type[] actual = pt.getActualTypeArguments();
				for (int a = 0; a < superTypeParameters.length; a++) {
					typeVariableNameMap.put(superTypeParameters[a].getName(),
							actual[a]);
				}
			}

			Class[] paramTypes = superConstructors[0].getParameterTypes();
			if (isNestedClassWithArtificialFirstArgument(superConstructors[0])) {
				paramTypes = removeFirstElement(Class.class, paramTypes);
				genericParams = removeFirstElement(Type.class, genericParams);
			}

			for (int a = 0; a < paramTypes.length; a++) {
				if (a > 0)
					cws.print(", ");
				if (genericParams[a] instanceof TypeVariable) {
					TypeVariable typeVar = (TypeVariable) genericParams[a];
					String name = typeVar.getName();
					Type t = typeVariableNameMap.get(name);
					String s = "(" + toString(cws.getNameMap(), t, false)
							+ ") null";
					cws.print(s);
				} else if (genericParams[a] instanceof GenericArrayType) {
					Type k = genericParams[a];
					String suffix = "";
					while (k instanceof GenericArrayType) {
						GenericArrayType gat = (GenericArrayType) k;
						k = gat.getGenericComponentType();
						suffix += "[]";
					}
					TypeVariable typeVar = (TypeVariable) k;
					String name = typeVar.getName();
					Type t = typeVariableNameMap.get(name);
					String s = "(" + toString(cws.getNameMap(), t, false)
							+ suffix + ") null";
					cws.print(s);
				} else {
					cws.print(getValue(cws.getNameMap(), paramTypes[a], true));
				}
			}
			cws.println(");");
		}
	}
}