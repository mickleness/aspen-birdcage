package org.abc.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * This is a Map.Entry implementation that is not attached to any Map.
 * <p>
 * This is similar to Aspen's KeyValuePair, but it implements {@link #hashCode()}
 * and {@link #equals(Object)}.
 */
public class BasicEntry<K, V> implements Map.Entry<K, V>, Serializable, Cloneable {
	
	private static final long serialVersionUID = 1L;
	
	K key;
	V value;
	
	public BasicEntry(K key, V value) {
		this.key = key;
		setValue(value);
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public V getValue() {
		return value;
	}

	@Override
	public V setValue(V value) {
		V oldValue = this.value;
		this.value = value;
		return oldValue;
	}

	@Override
	public int hashCode() {
		return key==null ? 0 : key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof Map.Entry))
			return false;
		@SuppressWarnings("rawtypes")
		Map.Entry other = (Map.Entry) obj;
		if(!Objects.equals(getKey(), other.getKey()))
			return false;
		if(!Objects.equals(getValue(), other.getValue()))
			return false;
		return true;
	}

	@Override
	protected BasicEntry<K, V> clone() {
		return new BasicEntry<K, V>(getKey(), getValue());
	}

	@Override
	public String toString() {
		return getClass().getSimpleName()+"[ "+getKey()+"="+getValue()+"]";
	}

	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		int version = in.readInt();
		if (version == 0) {
			key = (K) in.readObject();
			setValue( (V) in.readObject() );
		} else {
			throw new IOException("Unsupported internal version: " + version);
		}
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(0);
		out.writeObject(getKey());
		out.writeObject(getValue());
	}
	
}