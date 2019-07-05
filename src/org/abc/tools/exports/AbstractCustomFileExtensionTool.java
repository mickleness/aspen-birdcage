package org.abc.tools.exports;

import java.util.Random;

import com.follett.cust.cub.ExportHelperCub.FileType;
import com.follett.fsc.core.k12.tools.ToolJavaSource;
import com.x2dev.utils.X2BaseException;

/**
 * This helper class correctly manages a tool's file extension. You must call
 * {@link #setFileExtension(String)} early (preferably in
 * {@link #saveState(com.follett.fsc.core.k12.web.UserDataContainer)} for this
 * to work.
 */
public abstract class AbstractCustomFileExtensionTool extends ToolJavaSource {
	private static final long serialVersionUID = 1L;

	private String fileExtension;

	/**
	 * The file name of the output of this tool.
	 */
	private String customFileName;

	public void setFileExtension(String fileExtension) {
		if (customFileName != null)
			throw new IllegalStateException(
					"The file extension must be assigned before getCustomFileName() is called.");
		this.fileExtension = fileExtension;
	}

	public String getFileExtension() {
		return fileExtension;
	}

	@Override
	public String getCustomFileName() {
		initializeFileInfo();
		if (customFileName != null) {
			return customFileName;
		}
		return super.getCustomFileName();
	}

	private void initializeFileInfo() {
		if (customFileName == null) {
			if (fileExtension == null)
				throw new IllegalStateException(
						"setFileExtension(..) has not been called yet.");
			FileType f = FileType.forFileExtension(fileExtension);
			Random random = new Random();
			customFileName = "export" + random.nextInt(1000) + "."
					+ f.fileExtension;
			getJob().getInput().setFormat(f.toolInputType);
		}
	}

	@Override
	protected void initialize() throws X2BaseException {
		super.initialize();
		initializeFileInfo();
	}
}
