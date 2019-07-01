package org.abc.tools.exports;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.abc.tools.Tool;

import com.follett.fsc.core.framework.persistence.ColumnQuery;
import com.follett.fsc.core.k12.beans.ReportQueryIterator;
import com.follett.fsc.core.k12.tools.ToolJavaSource;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.pump.io.IOUtils;
import com.pump.xray.ClassWriter;
import com.pump.xray.ClassWriterStream;
import com.pump.xray.JarBuilder;
import com.pump.xray.SourceCodeManager;
import com.x2dev.sis.model.beans.SisDataTable;
import com.x2dev.utils.FolderUtils;

/**
 * This produces a zip archive of the java source code for x-ray jars. This
 * source code matches method signatures, classnames, and (simple) constants,
 * but it is otherwise empty.
 */
@Tool(id = "ABC-XR-EXP", name = "X-ray Jar Export", type = "export")
public class XrayExport extends ToolJavaSource {
	private static final long serialVersionUID = 1L;

	SourceCodeManager sourceCodeManager;

	@Override
	protected void run() throws Exception {
		sourceCodeManager = new SourceCodeManager();

		// tools
		sourceCodeManager
				.addClasses(com.x2dev.sis.tools.reports.StudentReportJavaSource.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.tools.reports.GradeReportJavaSource.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.tools.reports.ImmunizationReportJavaSource.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.tools.reports.ScheduleReportDataSource.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.tools.reports.ScheduleReportHelper.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.QuickChartData.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.Breakable.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.BaseFormReportJavaSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.QueryIteratorDataSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.imports.ImportJavaSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.imports.XmlDefinitionImport.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.imports.XmlDefinitionManager.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.imports.TextImportJavaSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.imports.XlsImportJavaSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.exports.XmlDefinitionExport.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.exports.ExportJavaSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.procedures.ProcedureJavaSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.procedures.DynamicFormProcedure.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.procedures.WorkflowProcedure.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.procedures.SessionAwareProcedure.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.procedures.QuickLetterData.class);

		// data sources
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.SecondaryStudentDataSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.SimpleFormDataSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.SimpleBeanDataSource.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.BeanCollectionDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports.engine.JRDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports3.engine.JRDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports5.engine.JRDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports.engine.data.JRBeanCollectionDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports3.engine.data.JRBeanCollectionDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports5.engine.data.JRBeanCollectionDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports.engine.JRException.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports3.engine.JRException.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports5.engine.JRException.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports.engine.JRRewindableDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports3.engine.JRRewindableDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports5.engine.JRRewindableDataSource.class);
		sourceCodeManager
				.addClasses(net.sf.jasperreports5.renderers.JFreeChartRenderer.class);

		// managers
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.business.StudentManager.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.business.PreferenceManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.EnrollmentManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.GraduationManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.AttendanceManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.ElementaryScheduleManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.GradeAverageManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.GradebookUpdateManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.GradePostManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.PdAttendanceManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.RecordAccessManagerSis.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.RubricStandardsInitializer.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.SisAnnouncementManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.SisStudentAlertManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.SisStudentManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.SpedNotificationManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.StaffClassSectionManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.ConductManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.CourseManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.StudentAttendanceManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.StudentDailyAttendanceManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.StudentPeriodAttendanceManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.StudentProgramParticipationManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.assessment.RubricManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.gradebook.GradebookManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.gradebook.GradebookImportManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.assignment.AssignmentManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.assignment.AssignmentAttachmentHelper.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.schedule.ScheduleManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.schedule.future.StudentScheduleChangeReportHelper.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.StaffAttendanceManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.CalendarManager.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.business.OrganizationManager.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.attendance.AttendanceManagerFactory.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.business.WriteEmailManager.class);

		// helpers
		sourceCodeManager
				.addClasses(com.x2dev.sis.tools.stateexports.StudentHistoryHelper.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.attendance.StaffAttendanceHelper.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.CalendarHelper.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.MultiStudentAttendanceSummary.class);

		// utils
		sourceCodeManager.addClasses(com.x2dev.utils.Base64.class);
		sourceCodeManager
				.addClasses(com.x2dev.utils.converters.ConverterFactory.class);
		sourceCodeManager.addClasses(com.lowagie.text.pdf.codec.Base64.class);
		sourceCodeManager.addClasses(com.x2dev.utils.DateUtils.class);
		sourceCodeManager
				.addClasses(org.apache.commons.lang3.time.DateUtils.class);
		sourceCodeManager.addClasses(org.apache.ojb.broker.util.Base64.class);
		sourceCodeManager.addClasses(com.x2dev.utils.StringUtils.class);
		sourceCodeManager
				.addClasses(org.apache.commons.lang3.StringUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.CollectionUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.ObjectUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.MapUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.StreamUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.ThreadUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.ZipUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.LoggerUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.FolderUtils.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.web.gradebook.GradeInputUtils.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.business.sped.SpedUtils.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.ReportUtils.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.web.AppGlobals.class);
		sourceCodeManager.addClasses(com.x2dev.utils.ZipUtils.class);
		sourceCodeManager.addClasses(com.x2dev.utils.NumberUtils.class);

		// constants
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.tools.reports.ReportConstants.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.business.ValidationConstants.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.beans.SisPreferenceConstants.class);
		sourceCodeManager.addClasses(com.x2dev.utils.UtilsConstants.class);
		sourceCodeManager.addClasses(com.x2dev.utils.UtilsGlobals.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.web.AppConstants.class);

		// misc
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.beans.StaffAttendanceSub.SubstituteType.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.web.SisUserDataContainer.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.web.gradebook.ScoreGrid.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.business.localization.LocalizationCache.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.business.PrivilegeGrid.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.framework.persistence.DatabaseOptimizer.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.framework.persistence.DatabaseOptimizerFactory.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.web.gradebook.LimitedColumnScoreGrid.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.k12.web.AppGlobals.class);
		sourceCodeManager
				.addClasses(com.x2dev.sis.model.beans.path.SisBeanPaths.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.framework.persistence.TempTable.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.framework.persistence.adjusters.DistinctAdjuster.class);
		sourceCodeManager
				.addClasses(com.follett.fsc.core.framework.persistence.RowResultIteratorBuilder.class);

		String[] columns = { SisDataTable.COL_CLASS_NAME };
		ColumnQuery query = new ColumnQuery(SisDataTable.class, columns,
				new org.apache.ojb.broker.query.Criteria());
		ReportQueryIterator iterator = getBroker()
				.getReportQueryIteratorByQuery(query);

		try {
			while (iterator.hasNext()) {
				Object[] object = (Object[]) iterator.next();
				String className = (String) object[0];
				Class z = Class.forName(className);
				sourceCodeManager.addClasses(z);
			}
		} finally {
			iterator.close();
		}

		publishResults();
	}

	protected void publishResults() throws Exception {
		// all this because ToolProvider.getSystemJavaCompiler returns null
		JarBuilder jarBuilder = new JarBuilder(sourceCodeManager) {
			@Override
			public void write(OutputStream out) throws Exception {
				Method mainMethod = null;
				try {
					Class<?> mainClass = Class
							.forName("com.sun.tools.javac.Main");
					mainMethod = mainClass.getMethod("compile", new Class[] {
							(new String[] {}).getClass(), PrintWriter.class });
				} catch (Exception e) {
					// this is really bad, but at least super.write(out) will
					// produce the .java files
					super.write(out);
					return;
				}

				File tempFolderRoot = AppGlobals.getRootTemporaryFolder();
				File tempFolder = FolderUtils
						.createUniqueFolder(tempFolderRoot);
				try {
					Collection<ClassWriter> classWriters = sourceCodeManager
							.build().values();

					List<String> argList = new ArrayList<>();
					argList.add("-classpath");
					argList.add(AppGlobals.getClasspath());

					for (ClassWriter writer : classWriters) {
						File file = createFile(tempFolder, writer.getType());
						try (OutputStream fileOut = new FileOutputStream(file)) {
							try (ClassWriterStream cws = new ClassWriterStream(
									fileOut, true, "UTF-8")) {
								writer.write(cws);
							}
						}
						argList.add(file.getAbsolutePath());
					}

					try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream()) {
						Object returnValue;
						try (PrintWriter writer = new PrintWriter(byteOut)) {
							String[] args = argList.toArray(new String[argList
									.size()]);
							returnValue = mainMethod.invoke(null, new Object[] {
									args, writer });
						}

						int returnInt = ((Number) returnValue).intValue();
						if (returnInt != 0) {
							throw new RuntimeException(returnInt + " "
									+ new String(byteOut.toByteArray()));
						}
					}

					writeJarFromDirectory(tempFolder, out);
				} finally {
					FolderUtils.recursiveRemoveDir(tempFolder, true);
				}
			}

			/**
			 * Return all the files in a directory.
			 * 
			 * @param file
			 *            the directory or file to consider.
			 * @return all the files in a directory.
			 */
			private List<File> getFiles(File file) {
				List<File> list = new ArrayList<>();
				if (file.isDirectory()) {
					File[] children = file.listFiles();
					for (File child : children) {
						list.addAll(getFiles(child));
					}
				} else {
					list.add(file);
				}
				return list;
			}

			/**
			 * Write a jar based on the directory provided.
			 * 
			 * @param dir
			 *            the root directory to model the jar from.
			 * @param out
			 *            the stream to write the jar data to.
			 * @throws IOException
			 */
			private void writeJarFromDirectory(File dir, OutputStream out)
					throws IOException {
				try (JarOutputStream jarOut = new JarOutputStream(out,
						getManifest())) {
					for (File file : getFiles(dir)) {
						if (!file.isDirectory()) {
							String p1 = file.getAbsolutePath();
							String p2 = dir.getAbsolutePath();
							p1 = p1.substring(p2.length());
							if (p1.startsWith(File.separator))
								p1 = p1.substring(1);
							p1 = p1.replace(File.separatorChar, '/');
							jarOut.putNextEntry(new JarEntry(p1));
							try (FileInputStream in = new FileInputStream(file)) {
								IOUtils.write(in, jarOut);
							}
						}
					}
				}
			}

			/**
			 * 
			 * @param rootDir
			 *            the root directory to write the file in.
			 * @param type
			 *            the class used to derive the file path. For example
			 *            the class "com.foo.Widget" will be stored in
			 *            "rootDir/com/foo/Widget.java"
			 * @return
			 * @throws IOException
			 */
			private File createFile(File rootDir, Class type)
					throws IOException {
				Package p = type.getPackage();
				String[] dirs = p.getName().split("\\.");
				File dir = rootDir;
				for (int a = 0; a < dirs.length; a++) {
					dir = new File(dir, dirs[a]);
					if (!dir.exists()) {
						dir.mkdir();
					}
				}
				File file = new File(dir, type.getSimpleName() + ".java");
				file.createNewFile();
				return file;
			}
		};
		jarBuilder
				.getManifest()
				.getMainAttributes()
				.putValue("Aspen-Version",
						com.follett.fsc.core.k12.web.AppGlobals.getVersion());
		jarBuilder.write(getResultHandler().getOutputStream());
	}

	@Override
	public String getCustomFileName() {
		return "aspen-xr-"
				+ com.follett.fsc.core.k12.web.AppGlobals.getVersion() + ".jar";
	}
}