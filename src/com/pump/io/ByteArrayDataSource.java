package com.pump.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.activation.DataSource;

/**
 * This DataSource is backed by a byte array.
 */
public class ByteArrayDataSource implements DataSource {
	protected byte[] data;
	protected String name, mimeType;

	public ByteArrayDataSource(byte[] data, String name, String mimeType) {
		if (data == null)
			throw new NullPointerException();
		if (name == null)
			throw new NullPointerException();
		if (mimeType == null)
			throw new NullPointerException();

		this.data = data;
		this.name = name;
		this.mimeType = mimeType;
	}

	@Override
	public String getContentType() {
		return mimeType;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return new ByteArrayInputStream(data);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		return new ByteArrayOutputStream() {

			@Override
			public void close() throws IOException {
				super.close();
				data = toByteArray();
			}
		};
	}
}