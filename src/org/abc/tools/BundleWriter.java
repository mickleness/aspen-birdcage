package org.abc.tools;

import java.awt.Toolkit;
import java.awt.datatransfer.StringSelection;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
					tool.input(), tool.category(), tool.nodes(),
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
			String inputXML, String category, String[] nodes, boolean output)
			throws Exception {
		File javaFile = context.getJavaFile(c.getName());
		File inputXMLFile = null;

		if (inputXML != null && !inputXML.isEmpty()) {
			inputXMLFile = new File(javaFile.getParentFile(), inputXML);
			if (!inputXMLFile.exists()) {
				throw new IllegalArgumentException("For tool \"" + c.getName()
						+ ", the input XML \"" + inputXML
						+ "\", was requested, but the file "
						+ inputXMLFile.getAbsolutePath() + " does not exist.");
			}
		} else {
			inputXMLFile = null;
		}

		Charset charset = Charset.forName("UTF-8");
		File target = new File(targetDir, toolName + ".bundle");
		String javaSource;
		try (FileOutputStream fileOut = new FileOutputStream(target)) {
			try (ZipOutputStream zipOut = new ZipOutputStream(fileOut)) {
				String inputXMLFilename = inputXMLFile == null ? null
						: inputXMLFile.getName();
				String bundleXml = createBundleXml(c, toolID, toolName,
						inputXMLFilename, category, nodes);
				zipOut.putNextEntry(new ZipEntry("bundle-definition.xml"));
				zipOut.write(bundleXml.getBytes(charset));

				String javaEntryName = c.getCanonicalName().replace(".", "/")
						+ ".java";
				zipOut.putNextEntry(new ZipEntry(javaEntryName));
				Breakout breakout = new Breakout(context, javaFile) {

					@Override
					protected boolean isJarSearchable(File jarFile) {
						String name = jarFile.getName().toLowerCase();
						return super.isJarSearchable(jarFile)
								&& !(name.contains("xr") || name.contains("lt"));
					}
				};
				// TODO: it'd be great if we compiled the java source code
				// against aspen xr and verified it was compiler-error-free
				javaSource = breakout.toString();
				zipOut.write(javaSource.getBytes(charset));

				if (inputXMLFilename != null) {
					String xmlEntryName = javaEntryName.substring(0,
							javaEntryName.lastIndexOf("/") + 1)
							+ inputXMLFilename;
					zipOut.putNextEntry(new ZipEntry(xmlEntryName));
					IOUtils.write(inputXMLFile, zipOut);
				}
			}
		}
		if (output)
			System.out.println("Wrote " + target.getAbsolutePath() + " ("
					+ IOUtils.formatFileSize(target) + ")");
		return javaSource;
	}

	private String createBundleXml(Class c, String toolID, String toolName,
			String inputXMLFilename, String category, String[] nodes) {

		// TODO: it'd be great if we validated this XML, too. There's a lot that
		// we're injecting that could be either bad XML, or violate the tool
		// input's dtd.

		List<String> nodeList = new ArrayList<>();
		if (nodes != null) {
			for (String node : nodes) {
				if (node != null && !node.isEmpty()) {
					nodeList.add(node);
				}
			}
		}

		StringBuilder sb = new StringBuilder();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		String date = new SimpleDateFormat("YYYY-MM-dd").format(new Date());
		sb.append("<tool-bundle create-date=\"" + date + "\">\n");

		ToolType type = getToolType(c);
		sb.append("  <" + type.plural + " package=\""
				+ c.getPackage().getName() + "\">\n");
		sb.append("    <" + type.single + " id=\"" + toolID + "\" name=\""
				+ toolName + "\" javasource-file=\"" + c.getSimpleName()
				+ ".java\"");
		if (inputXMLFilename != null && inputXMLFilename.length() > 0) {
			sb.append(" input-file=\"" + inputXMLFilename + "\"");
		}
		if (category != null && category.length() > 0) {
			sb.append(" category=\"" + category + "\"");
		}

		if (nodeList.size() == 0) {
			sb.append("/>\n");
		} else {
			sb.append(">\n");
			for (String node : nodeList) {
				sb.append("      <node " + node + "/>\n");
			}
			sb.append("    </" + type.single + ">\n");
		}

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
