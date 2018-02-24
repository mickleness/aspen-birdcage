package com.pump.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This writes data to a temporary file adjacent to the real file, and then when
 * {@link #close()} is called we rename the temp file to the intended target
 * file name.
 * <p>
 * This prevents us from writing to the actual target file until we're sure we
 * have a viable file to replace it with. This is a common practice when saving
 * a document: in case something goes wrong saving, you never want to ALSO
 * destroy the original file.
 */
public class FileSwitchWriter implements AutoCloseable {
	protected File dest, temp;
	private OutputStream fileOut;

	/**
	 * Create a new FileSwitchWriter.
	 * 
	 * @param dest
	 *            the File we eventually want our data written to.
	 */
	public FileSwitchWriter(File dest) throws IOException {
		if (dest == null)
			throw new NullPointerException();

		this.dest = dest;
		this.temp = new File(dest.getParentFile(), "." + dest.getName()
				+ ".temp");
		if (temp.exists())
			throw new IllegalArgumentException("The file \""
					+ temp.getAbsolutePath() + "\" already exists");
		temp.createNewFile();
	}

	/**
	 * Create an OutputStream to write file data to.
	 * <P>
	 * This actually writes to an adjacent temp file, and the filenames are
	 * switched when {@link #close()} is called.
	 */
	public OutputStream createOutputStream() throws FileNotFoundException {
		if (fileOut == null) {
			fileOut = new FileOutputStream(temp);
			return fileOut;
		}
		throw new IllegalStateException(
				"Illegal redundant call to createOutputStream()");
	}

	@Override
	protected void finalize() throws IOException {
		try {
			if (fileOut != null) {
				fileOut.close();
				fileOut = null;
			}
		} finally {
			if (temp.exists())
				temp.delete();
		}
	}

	/**
	 * This method replaces the original file with the temporary file this
	 * object wrote.
	 */
	@Override
	public void close() throws Exception {
		if (dest.exists())
			dest.delete();
		if (fileOut != null) {
			fileOut.close();
			fileOut = null;
		}
		if (temp.exists()) {
			temp.renameTo(dest);
		}
	}
}
