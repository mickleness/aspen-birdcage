package org.abc.tools;

import java.awt.Toolkit;
import java.awt.datatransfer.StringSelection;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.follett.fsc.core.k12.tools.exports.ExportJavaSource;
import com.follett.fsc.core.k12.tools.imports.ImportJavaSource;
import com.follett.fsc.core.k12.tools.procedures.ProcedureJavaSource;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSource;
import com.pump.breakout.Breakout;
import com.pump.breakout.WorkspaceContext;
import com.pump.io.IOUtils;

/**
 * This writes all the available tools that include the {@link Tool} annotation
 * as Aspen bundles.
 */
public class BundleWriter {

	/**
	 * The bundle type (as declared in the bundle-definition.xml file).
	 */
	public enum ToolType {
		IMPORT("import-definition", "imports", ImportJavaSource.class), EXPORT(
				"export-definition", "exports", ExportJavaSource.class), PROCEDURE(
				"procedure-definition", "procedures", ProcedureJavaSource.class), REPORT(
				"report-definition", "reports", ReportJavaSource.class);
		String single, plural;
		Class superclass;

		ToolType(String single, String plural, Class superclass) {
			this.single = single;
			this.plural = plural;
			this.superclass = superclass;
		}
	}

	/**
	 * This walks through the current working directory and creates a bundled
	 * zip of every tool that has the {@link Bundle} annotation.
	 */
	public static void main(String[] args) throws Exception {
		BundleWriter app = new BundleWriter();
		app.run();
	}

	protected WorkspaceContext context;
	protected File sourcePath;
	protected File targetDir;
	protected Class[] tools;

	/**
	 * Create a BundleWriter.
	 * 
	 * @param tools
	 *            if you pass nothing then ALL tools will be scanned and
	 *            written. If you pass a specific set of tools: only those
	 *            bundles will be written. If you pass exactly 1 tool, then the
	 *            resulting source code is also copied to the clipboard and
	 *            printed to the console.
	 * 
	 * @throws IOException
	 */
	public BundleWriter(Class... tools) throws IOException {
		context = new WorkspaceContext();
		sourcePath = context.getSourcePaths().iterator().next();
		targetDir = new File(sourcePath, "target");
		targetDir = new File(targetDir, "bundles");
		this.tools = tools;
	}

	public void run() throws Exception {
		if (tools != null && tools.length > 0) {
			for (Class c : tools) {
				run(c, tools.length == 1);
			}
		} else {
			for (String classname : context.getClassNames()) {
				Class c = Class.forName(classname);
				run(c, false);
			}
		}
	}

	protected void run(Class c, boolean copyToClipboard) throws Exception {
		Tool tool = (Tool) c.getAnnotation(Tool.class);
		if (tool != null) {
			String javaSource = writeBundle(c, tool.id(), tool.name(),
					!copyToClipboard);
			if (copyToClipboard) {
				StringSelection selection = new StringSelection(javaSource);
				Toolkit.getDefaultToolkit().getSystemClipboard()
						.setContents(selection, selection);
				System.out.println(javaSource);
			}
		}
	}

	private String writeBundle(Class c, String toolID, String toolName,
			boolean output) throws Exception {
		File javaFile = context.getJavaFile(c.getName());

		Charset charset = Charset.forName("UTF-8");
		File target = new File(targetDir, toolName + ".zip");
		String javaSource;
		try (FileOutputStream fileOut = new FileOutputStream(target)) {
			try (ZipOutputStream zipOut = new ZipOutputStream(fileOut)) {
				String bundleXml = createBundleXml(c, toolID, toolName);
				zipOut.putNextEntry(new ZipEntry("bundle-definition.xml"));
				zipOut.write(bundleXml.getBytes(charset));
				zipOut.putNextEntry(new ZipEntry(c.getCanonicalName().replace(
						".", "/")
						+ ".java"));
				Breakout breakout = new Breakout(context, javaFile);
				// TODO: it'd be great if we compiled the java source code
				// against aspen xr and verified it was compiler-error-free
				javaSource = breakout.toString();
				zipOut.write(javaSource.getBytes(charset));
			}
		}
		if (output)
			System.out.println("Wrote " + target.getAbsolutePath() + " ("
					+ IOUtils.formatFileSize(target) + ")");
		return javaSource;
	}

	private String createBundleXml(Class c, String toolID, String toolName) {
		StringBuilder sb = new StringBuilder();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		String date = new SimpleDateFormat("YYYY-MM-dd").format(new Date());
		sb.append("<tool-bundle create-date=\"" + date + "\">\n");

		ToolType type = getToolType(c);
		sb.append("  <" + type.plural + " package=\""
				+ c.getPackage().getName() + "\">\n");
		// TODO: include 'input-file="PdfSimulatorJavaSourceInput.xml"'
		sb.append("    <" + type.single + " id=\"" + toolID + "\" name=\""
				+ toolName + "\" javasource-file=\"" + c.getSimpleName()
				+ ".java" + "\"" + "/>\n");
		sb.append("  </" + type.plural + ">\n");

		sb.append("</tool-bundle>");

		return sb.toString();
	}

	private ToolType getToolType(Class c) {
		Tool bundle = (Tool) c.getAnnotation(Tool.class);
		String type = bundle.type();
		for (ToolType bundleType : ToolType.values()) {
			if (bundleType.name().equalsIgnoreCase(type))
				return bundleType;
		}

		Class t = c;
		while (c != null) {
			for (ToolType bundleType : ToolType.values()) {
				if (bundleType.superclass.equals(c)) {
					return bundleType;
				}
			}
			c = c.getSuperclass();
		}
		throw new IllegalArgumentException("The class " + t.getCanonicalName()
				+ " didn't extend one of the supported tool types.");
	}
}
