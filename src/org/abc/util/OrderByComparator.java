package org.abc.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.ojb.broker.metadata.FieldHelper;

import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.ModelProperty;

/**
 * This Comparator sorts elements according to a series of
 * org.apache.ojb.broker.metadata.FieldHelpers.
 * <p>
 * The oid of the beans serves as a tie-breaker if all the requested fields are
 * identical.
 */
public class OrderByComparator implements Comparator<X2BaseBean>, Serializable {
	
	private static final long serialVersionUID = 1L;

	/**
	 * This sorts two beans according to the value of a particular field
	 * ("bean value").
	 * <p>
	 * It is safe to assume the {@link #compare(X2BaseBean, X2BaseBean)} method
	 * will never be passed a null bean.
	 */
	protected static class NullSafeFieldHelper extends FieldHelper implements
			Comparator<X2BaseBean> {
		private static final long serialVersionUID = 1L;

		public boolean nullValueFirst;

		/**
		 * Create a new NullSafeFieldHelper
		 * 
		 * @param fieldName
		 *            the name of the field/value to retrieve from the bean
		 * @param orderAscending
		 *            true if the value should sorted in ascending order
		 * @param nullValueFirst
		 *            true if null values should be stored at the beginning of
		 *            the set, or false if null values should be stored at the
		 *            end of the set.
		 */
		public NullSafeFieldHelper(String fieldName, boolean orderAscending,
				boolean nullValueFirst) {
			super(fieldName, orderAscending);
			this.nullValueFirst = nullValueFirst;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public int compare(X2BaseBean o1, X2BaseBean o2) {
			Comparable v1 = (Comparable) o1.getFieldValueByBeanPath(name);
			Comparable v2 = (Comparable) o2.getFieldValueByBeanPath(name);
			if (v1 == null && v2 == null) {
				return 0;
			} else if (nullValueFirst) {
				if (v1 == null)
					return -1;
				if (v2 == null)
					return 1;
			} else {
				if (v1 == null)
					return 1;
				if (v2 == null)
					return -1;
			}
			int k = v1.compareTo(v2);
			if (!isAscending)
				k = -k;
			return k;
		}

		@Override
		public int hashCode() {
			return name.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof NullSafeFieldHelper))
				return false;
			NullSafeFieldHelper other = (NullSafeFieldHelper) obj;
			if (!Objects.equals(name, other.name))
				return false;
			if (isAscending != other.isAscending)
				return false;
			if (nullValueFirst != other.nullValueFirst)
				return false;
			return true;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("NullSafeFieldHelper[ \"");
			sb.append(name);
			sb.append("\", ");
			sb.append(isAscending ? "ascending" : "descending");
			sb.append(", ");
			sb.append(nullValueFirst ? "null value first" : "null value last");
			sb.append("]");
			return sb.toString();
		}

		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.writeInt(0);
			out.writeBoolean(nullValueFirst);
		}

		private void readObject(java.io.ObjectInputStream in) throws IOException,
				ClassNotFoundException {
			int version = in.readInt();
			if (version == 0) {
				nullValueFirst = in.readBoolean();
			} else {
				throw new IOException("Unsupported internal version: " + version);
			}
		}
	}

	protected List<NullSafeFieldHelper> fieldHelpers = new LinkedList<>();
	protected boolean nullsFirst;

	/**
	 * Create a new OrderByComparator.
	 * 
	 * @param nullsFirst
	 *            if true then null beans are stored at the beginning of the
	 *            set, if false then they're stored at the end of the set.
	 *            Needing to accommodate a null bean is probably very rare.
	 *            (This is not to be confused with accommodating a null value
	 *            for a field, which is much more likely.)
	 */
	public OrderByComparator(boolean nullsFirst) {
		this.nullsFirst = nullsFirst;
	}

	/**
	 * Create a new OrderByComparator.
	 * 
	 * @param nullsFirst
	 *            if true then null beans are stored at the beginning of the
	 *            set, if false then they're stored at the end of the set.
	 *            Needing to accommodate a null bean is probably very rare.
	 *            (This is not to be confused with accommodating a null value
	 *            for a field, which is much more likely.)
	 * @param fieldHelpers
	 *            a list of FieldHelpers, probably from calling
	 *            {@link org.apache.ojb.broker.query.Query#getOrderBy()}.
	 */
	public OrderByComparator(boolean nullsFirst, List<FieldHelper> fieldHelpers) {
		this.nullsFirst = nullsFirst;
		for (FieldHelper h : fieldHelpers) {
			boolean fieldNullFirst = false;
			if (h instanceof NullSafeFieldHelper) {
				fieldNullFirst = ((NullSafeFieldHelper) h).nullValueFirst;
			}
			addOrderBy(h.name, h.isAscending, fieldNullFirst);
		}
	}

	/**
	 * Add a new "order-by" instruction to this comparator.
	 * <p>
	 * These instructions are evaluated in the order they are added to this
	 * OrderByComparator.
	 * 
	 * @param name
	 *            the name of the field to sort by. This String is the argument
	 *            passed to {@link X2BaseBean#getFieldValueByBeanPath(String)}.
	 * @param isAscending
	 *            true if the values should be sorted in ascending order. False
	 *            if the values should be in descending order.
	 * @param nullValueFirst
	 *            if true then null values are stored at the beginning of the
	 *            results. If false then null values are stored at the end of
	 *            the results.
	 */
	public synchronized void addOrderBy(String name, boolean isAscending,
			boolean nullValueFirst) {
		fieldHelpers.add(new NullSafeFieldHelper(name, isAscending,
				nullValueFirst));
		isSimple = null;
	}

	@Override
	public synchronized int compare(X2BaseBean o1, X2BaseBean o2) {
		if (o1 == o2)
			return 0;

		if (nullsFirst) {
			if (o1 == null)
				return -1;
			if (o2 == null)
				return 1;
		} else {
			if (o1 == null)
				return 1;
			if (o2 == null)
				return -1;
		}

		for (NullSafeFieldHelper h : fieldHelpers) {
			int k = h.compare(o1, o2);
			if (k != 0)
				return k;
		}
		return o1.getOid().compareTo(o2.getOid());
	}

	@Override
	public synchronized int hashCode() {
		return fieldHelpers.hashCode();
	}

	@Override
	public synchronized boolean equals(Object obj) {
		if (!(obj instanceof OrderByComparator))
			return false;
		OrderByComparator other = (OrderByComparator) obj;
		if (other.nullsFirst != nullsFirst)
			return false;
		if (!other.fieldHelpers.equals(fieldHelpers))
			return false;
		return true;
	}

	@Override
	public synchronized String toString() {
		return "OrderByComparator[ fields=" + fieldHelpers + ", nullsFirst="
				+ nullsFirst + "]";
	}

	/**
	 * Return a copy of the FieldHelpers this object uses.
	 */
	public synchronized List<FieldHelper> getFieldHelpers() {
		List<FieldHelper> returnValue = new ArrayList<FieldHelper>();
		returnValue.addAll(fieldHelpers);
		return returnValue;
	}

	private transient Boolean isSimple;
	
	/**
	 * Return true if all of the fields in this comparator do not contain
	 * ModelPropert.PATH_DELIMITER.
	 * <p>
	 * For example: if this comparator relates to SisStudent beans,
	 * then "nameView" and "homeroom" and "personOid" are considered simple,
	 * but "person.lastName" is not.
	 * <p>
	 * Evaluating a simple property should not require any additional
	 * work on the database's part. Evaluating a non-simple property
	 * may-or-may-not involve consulting the database.
	 */
	public synchronized boolean isSimple() {
		if(isSimple==null) {
			for(NullSafeFieldHelper h : fieldHelpers) {
				if(h.name.indexOf(ModelProperty.PATH_DELIMITER)!=-1)
					isSimple = Boolean.FALSE;
			}
			if(isSimple==null)
				isSimple = Boolean.TRUE;
		}
		return isSimple.booleanValue();
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(0);
		out.writeBoolean(nullsFirst);
		out.writeObject(fieldHelpers);
	}

	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		int version = in.readInt();
		if (version == 0) {
			nullsFirst = in.readBoolean();
			fieldHelpers = (List<NullSafeFieldHelper>) in.readObject();
		} else {
			throw new IOException("Unsupported internal version: " + version);
		}
	}
}
