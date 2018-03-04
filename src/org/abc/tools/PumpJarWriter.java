package org.abc.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.pump.breakout.WorkspaceContext;
import com.pump.io.FileTreeIterator;
import com.pump.io.IOUtils;

/**
 * This collapses all the .class and .java files in the "com.pump" packages into
 * one helper jar file.
 * <p>
 * There is nothing necessarily wrong with having other code, including com.pump
 * code, lying around in this project. But at least for now I like the idea of
 * keeping the .java file more strictly limited to with Aspen tools or the
 * direct maintenance of this project.
 * <p>
 * TODO: if we ever want to update/maintain those com.pump classes, it'd be
 * great if this app could help modifier the existing jar. Currently if we need
 * to make changes the thing to do is expand the jar file and then re-run this
 * app when we're finished.
 */
public class PumpJarWriter {
	public static void main(String[] args) throws Exception {
		PumpJarWriter w = new PumpJarWriter();
		w.run();
	}

	WorkspaceContext context;

	public void run() throws Exception {
		context = new WorkspaceContext();
		File dest = new File("PumpClasses.jar");
		try (FileOutputStream fileOut = new FileOutputStream(dest)) {
			try (ZipOutputStream jarOut = new ZipOutputStream(fileOut)) {
				for (File srcPath : context.getSourcePaths()) {
					FileTreeIterator iter = new FileTreeIterator(srcPath,
							"class", "java");
					while (iter.hasNext()) {
						File file = iter.next();
						String p = file.getAbsolutePath();
						p = p.replace(File.separatorChar, '/');
						int i = p.indexOf("com/pump/");
						if (i != -1) {
							p = p.substring(i);
							jarOut.putNextEntry(new ZipEntry(p));
							IOUtils.write(file, jarOut);
						}
					}
				}
			}
		}
		System.out.println("Wrote: " + dest.getAbsolutePath() + " "
				+ IOUtils.formatFileSize(dest));
	}
}
