/*
 * seth for LS from Steven Lyle
 * adjusted in following ways
 *    • Document is of type PDF
 *    • filename not have ID or Date
 *    • store uploadUser printName
 */

package procedures;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import org.apache.commons.lang3.StringUtils;

import com.follett.fsc.core.k12.beans.Document;
import com.follett.fsc.core.k12.beans.Organization;
import com.follett.fsc.core.k12.beans.Person;
import com.follett.fsc.core.k12.beans.Report;
import com.follett.fsc.core.k12.beans.Staff;
import com.follett.fsc.core.k12.beans.Student;
import com.follett.fsc.core.k12.beans.User;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.FileAttachmentManager;
import com.follett.fsc.core.k12.business.JobLogManager;
import com.follett.fsc.core.k12.business.ValidationError;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.business.dictionary.DataDictionary;
import com.follett.fsc.core.k12.business.dictionary.DataDictionaryField;
import com.follett.fsc.core.k12.tools.Tool;
import com.follett.fsc.core.k12.tools.ToolJob;
import com.follett.fsc.core.k12.tools.reports.Breakable;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.x2dev.utils.DataGrid;
import com.x2dev.utils.LoggerUtils;
import com.x2dev.utils.types.PlainDate;

import net.sf.jasperreports.engine.JRException;

/**
 * Class to handle document creation.
 * 
 * In the Parent report input XML, we need to the following XML, this will allow
 * us to choose when to create document:
 * 
 * <!-- **************** --> <!-- CREATE DOCUMENTS --> <!-- **************** -->
 * 
 * <input name="createDocuments" data-type="boolean" display-type="checkbox"
 * default-value="false" display-name="Create Documents" />
 * 
 * <!-- ************************ --> <!-- OPTIONAL CUSTOM FILENAME --> <!--
 * ************************ -->
 * 
 * <input name="filename" data-type="string" display-type="text" display-name=
 * "Optional Filename" />
 * 
 * <!-- ********************** --> <!-- OPTIONAL DOCUMENT TYPE --> <!--
 * ********************** -->
 * 
 * <input name="docTypeOid" data-type="string" display-type="reference"
 * reference-table="rtbDocType " display-name="Optional Document Type" required=
 * "false" />
 * 
 * ------------------------------------------------------------
 * 
 * We will have to add an import to the top of the Java file:
 * 
 * import procedures.DocumentManager;
 * 
 * When the parent report returns the grid, we should intercept there with these
 * lines:
 * 
 * DocumentManager docManager = new DocumentManager(getBroker(),
 * getOrganization(), getJob(), getUser()); docManager.storeDocuments(grid,
 * getFormat(), getParameters(), DocumentManager.STUDENT_COLUMN);
 * 
 * This will take the grid and generate a document for each break column and
 * write it to their documents.
 * 
 * Then the grid will be returned to the main report to function as normal.
 */
public class DocumentManager {

	/**
	 * This ExecutorService processes tasks immediately; it does not use any
	 * other threads.
	 */
	public static class InlineExecutorService implements ExecutorService {
		static class SimpleFuture<T> implements Future<T> {

			T value;
			Throwable throwable;

			public SimpleFuture(T value) {
				this.value = value;
			}

			public SimpleFuture(Throwable throwable) {
				this.throwable = throwable;
			}

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				return false;
			}

			@Override
			public boolean isCancelled() {
				return false;
			}

			@Override
			public boolean isDone() {
				return true;
			}

			@Override
			public T get() throws InterruptedException, ExecutionException {
				if (throwable != null)
					throw new ExecutionException("", throwable);
				return value;
			}

			@Override
			public T get(long timeout, TimeUnit unit)
					throws InterruptedException, ExecutionException,
					TimeoutException {
				return get();
			}

		}

		boolean shutdown = false;
		boolean terminated = false;

		@Override
		public void execute(Runnable command) {
			if (isShutdown())
				throw new RejectedExecutionException();
			if (command != null)
				command.run();
		}

		@Override
		public void shutdown() {
			shutdown = true;
		}

		@Override
		public List<Runnable> shutdownNow() {
			shutdown = true;
			return new LinkedList<>();
		}

		@Override
		public boolean isShutdown() {
			return shutdown;
		}

		@Override
		public boolean isTerminated() {
			return terminated;
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit)
				throws InterruptedException {
			return true;
		}

		@Override
		public <T> Future<T> submit(Callable<T> task) {
			T result;
			try {
				result = task.call();
				return new SimpleFuture<T>(result);
			} catch (Exception e) {
				return new SimpleFuture<T>(e);
			}
		}

		@Override
		public <T> Future<T> submit(Runnable task, T result) {
			task.run();
			return new SimpleFuture<T>(result);
		}

		@Override
		public Future<?> submit(Runnable task) {
			return submit(task, null);
		}

		@Override
		public <T> List<Future<T>> invokeAll(
				Collection<? extends Callable<T>> tasks)
				throws InterruptedException {
			List<Future<T>> returnValue = new ArrayList<>();
			for (Callable<T> c : tasks) {
				try {
					T result = c.call();
					returnValue.add(new SimpleFuture<>(result));
				} catch (Exception e) {
					returnValue.add(new SimpleFuture<T>(e));
				}
			}
			return returnValue;
		}

		@Override
		public <T> List<Future<T>> invokeAll(
				Collection<? extends Callable<T>> tasks, long timeout,
				TimeUnit unit) throws InterruptedException {
			long maxMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
			long startTime = System.currentTimeMillis();
			List<Future<T>> returnValue = new ArrayList<>();
			for (Callable<T> c : tasks) {
				long elapsed = System.currentTimeMillis() - startTime;
				try {
					if (elapsed > maxMillis)
						throw new TimeoutException("invokeAll timeout after "
								+ timeout + " " + unit + "(s)");
					T result = c.call();
					returnValue.add(new SimpleFuture<>(result));
				} catch (Exception e) {
					returnValue.add(new SimpleFuture<T>(e));
				}
			}
			return returnValue;
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
				throws InterruptedException, ExecutionException {
			if (tasks.isEmpty())
				throw new IllegalArgumentException();
			Iterator<? extends Callable<T>> iter = tasks.iterator();
			try {
				return iter.next().call();
			} catch (Exception e) {
				throw new ExecutionException(e);
			} finally {
				// I'm unclear from the documentation if we're supposed to purge
				// this?
				while (iter.hasNext()) {
					iter.next();
				}
			}
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
				long timeout, TimeUnit unit) throws InterruptedException,
				ExecutionException, TimeoutException {
			// we can't know if we run past the time limit until
			// we've processed a task, so the time limit is useless.
			return invokeAny(tasks);
		}
	}

	/**
	 * This ThreadPoolSubreportRunnerFactory uses a InlineExecutorService to
	 * avoid multithreading.
	 *
	 */
	public static class InlineThreadPoolSubreportRunnerFactory extends
			net.sf.jasperreports5.engine.fill.ThreadPoolSubreportRunnerFactory {
		@Override
		protected java.util.concurrent.ExecutorService createThreadExecutor(
				net.sf.jasperreports5.engine.fill.JRFillContext fillContext) {
			return new InlineExecutorService();
		}
	}

	protected static final int MAX_DEPTH = 4;

	protected static final String LOG_LABEL = "DOCUMENT MANAGER: ";
	protected static final String PDF_EXTENSION = ".pdf";

	public static final String STUDENT_COLUMN = "student";
	public static final String STAFF_COLUMN = "staff";
	public static final String PERSON_COLUMN = "person";

	/**
	 * Name for the parameter that controls creating documents
	 */
	public static final String CREATE_DOCS_PARAM = "createDocuments";
	/*
	 * <b>Optional: User defined fields</b> - You can set up user defined fields
	 * on the PERSON_DOCUMENT table to store the following values: <ul>
	 * <li><u>Upload Date</u> - Use the alias <i>upload-date</i> to store the
	 * date that the document was uploaded to the student.</li> <li><u>School
	 * ID</u> - Use the alias <i>school-id</i> to save the school ID into a UDF
	 * on the document</li> <li><u>Report OID</u> - Use the alias
	 * <i>report-oid</i> to save the report's OID into a UDF on the
	 * document</li> <li><u>User OID</u> - Use the alias <i>user-oid</i> to save
	 * the OID of the user running the report into a UDF on the document</li>
	 * </ul>
	 */
	protected static final String ALIAS_REPORT_OID = "report-oid";
	protected static final String ALIAS_SCHOOL_ID = "school-id";
	protected static final String ALIAS_UPLOAD_DATE = "upload-date";
	protected static final String ALIAS_USER_OID = "user-oid";

	// Data Dictionary Fields
	protected DataDictionary dictionary;
	protected DataDictionaryField docNameField;
	protected DataDictionaryField fileNameField;
	protected DataDictionaryField reportOidField;
	protected DataDictionaryField schoolIdField;
	protected DataDictionaryField uploadDateField;
	protected DataDictionaryField userOidField;

	// seth for upLoaded By Name
	protected static final String ALIAS_UPLOADED_BY = "uploaded-by";
	protected DataDictionaryField uploadedByField;
	protected Staff userStaff;
	protected String userStaffPrintName;

	// save options
	protected boolean saveToDatabase;
	protected boolean saveToDisk;

	// broker
	protected X2Broker broker;

	// organization
	protected Organization organization;

	// tool job
	protected ToolJob job;

	// user
	protected User user;

	// custom filename
	protected String customFilename;

	// document type code
	protected String docTypeCode;

	// input stream in format, must reset on each use
	protected HashSet<InputStream> streams;

	// information about the break column
	@SuppressWarnings("rawtypes")
	Class breakColClass;

	/**
	 * Create a Document Manager instance, default to saving file on disk with
	 * default file name
	 */
	public DocumentManager(X2Broker broker, Organization organization,
			ToolJob job, User user) {
		this(broker, organization, job, user, false, true, null, null);
	}

	/**
	 * Create a Document Manager instance, default to saving file on disk with
	 * custom file name
	 */
	public DocumentManager(X2Broker broker, Organization organization,
			ToolJob job, User user, String customFilename, String docTypeCode) {
		this(broker, organization, job, user, false, true, customFilename,
				docTypeCode);
	}

	/**
	 * Create a Document Manager instance, default to saving file on disk with
	 * custom file name
	 */
	public DocumentManager(X2Broker broker, Organization organization,
			ToolJob job, User user, String customFilename) {
		this(broker, organization, job, user, false, true, customFilename,
				null);
	}

	/**
	 * Create a Document Manager instance with default file name
	 */
	public DocumentManager(X2Broker broker, Organization organization,
			ToolJob job, User user, boolean saveToDatabase,
			boolean saveToDisk) {
		this(broker, organization, job, user, saveToDatabase, saveToDisk, null,
				null);
	}

	/**
	 * Create a Document Manager instance
	 */
	public DocumentManager(X2Broker broker, Organization organization,
			ToolJob job, User user, boolean saveToDatabase, boolean saveToDisk,
			String customFilename, String docTypeCode) {
		this.broker = broker;
		if (broker != null) {
			this.docTypeCode = docTypeCode;
			this.saveToDatabase = saveToDatabase;
			this.saveToDisk = saveToDisk;
			this.organization = organization;
			this.job = job;
			this.user = user;
			this.customFilename = customFilename;
			dictionary = DataDictionary
					.getDistrictDictionary(broker.getPersistenceKey());
			docNameField = dictionary.findDataDictionaryField(
					Document.class.getName(), Document.COL_NAME);
			fileNameField = dictionary.findDataDictionaryField(
					Document.class.getName(), Document.COL_FILENAME);
			reportOidField = dictionary
					.findDataDictionaryFieldByAlias(ALIAS_REPORT_OID);
			schoolIdField = dictionary
					.findDataDictionaryFieldByAlias(ALIAS_SCHOOL_ID);
			uploadDateField = dictionary
					.findDataDictionaryFieldByAlias(ALIAS_UPLOAD_DATE);
			userOidField = dictionary
					.findDataDictionaryFieldByAlias(ALIAS_USER_OID);

			// seth
			uploadedByField = dictionary
					.findDataDictionaryFieldByAlias(ALIAS_UPLOADED_BY);

			streams = new HashSet<>();
		}
	}

	/**
	 * Main method we call to process the data into documents in the student's
	 * side tabs.
	 */
	public void storeDocuments(Object inputGrid, InputStream format,
			Map<String, Object> params, String breakCol) {
		// make sure we have checked create documents
		boolean createDocuments = false;
		if (params.get(CREATE_DOCS_PARAM) != null) {
			createDocuments = ((Boolean) params.get(CREATE_DOCS_PARAM))
					.booleanValue();
		} else {
			logMessage(
					"the createDocuments boolean input variable is missing. This must exist in order to select \"create documents\" on the input of this report.");
		}

		// if we aren't creating documents then return.
		if (!createDocuments) {
			return;
		}

		ReportDataGrid grid = (ReportDataGrid) inputGrid;

		// check if our variables were all passed correctly, if not return
		if (!verifyParameters(grid, format, breakCol)) {
			return;
		}

		// initialize document count
		int documentCount = 0;

		// Builds a list of streams so that we can reset them
		streams.add(format);
		streams.addAll(getParamStreams(params));

		// make sure all our break columns are unique
		HashSet<Object> uniqueBreakColumns = new HashSet<>();

		// boolean to indicate we finished try{} with success
		boolean success = false;

		// determine whether we are in a transaction, if not start one.
		boolean brokerInTransaction = broker.isInTransaction();
		try {
			if (!brokerInTransaction) {
				broker.beginTransaction();
			}

			// split the grid and generate each file separately
			for (Object reportData : grid.getDataSources(breakCol)) {
				// get object who is in the break column of this grid
				Object bean = ((Breakable) reportData).getBreakValue();

				// make sure we only use a break column once.
				if (!uniqueBreakColumns.add(bean)) {
					String msg = "This break column [" + bean
							+ "] is a duplicate, break columns must be sorted into consecutive groups in the grid.";
					logMessage(msg);
					throw new RuntimeException(msg);
				}

				// determine the class of the break column
				if (bean instanceof Student)
					breakColClass = Student.class;
				if (bean instanceof Staff)
					breakColClass = Staff.class;
				if (bean instanceof Person)
					breakColClass = Person.class;

				if (breakColClass == null) {
					String msg = "This break column is unknown, we support student, staff, and person: "
							+ breakCol;
					logMessage(msg);
					throw new RuntimeException(msg);
				}

				// generate PDF
				byte[] pdfBytes = generatePdf((ReportDataGrid) reportData,
						format, params);

				createDocument(bean, pdfBytes);
				documentCount++;

				logMessage("created " + documentCount + " documents.");
				success = true;
			}
		} catch (JRException | net.sf.jasperreports3.engine.JRException
				| net.sf.jasperreports5.engine.JRException jre) {
			throw new RuntimeException("Error generating PDF: "
					+ LoggerUtils.convertThrowableToString(jre));
		} catch (IOException ioe) {
			throw new RuntimeException("IOException : "
					+ LoggerUtils.convertThrowableToString(ioe));
		} finally {
			if (!brokerInTransaction && success) {
				broker.commitTransaction();
				logMessage(
						"Document creation was successful, committing data to the database.");
			} else if (!brokerInTransaction && !success) {
				broker.rollbackTransaction();
				logMessage(
						"An error occurred, document creation was rolled back.");
			}
			grid.beforeTop();
		}
	}

	/**
	 * Return InputStreams inside parameters
	 *
	 * @param params
	 *
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	protected Collection<InputStream> getParamStreams(
			Map<String, Object> params) {
		Collection<InputStream> paramStreams = new ArrayList<>();
		// get input streams for reset from format, parameters, and grid object
		for (Object parameter : params.keySet()) {
			Object value = params.get(parameter);
			if (value instanceof ByteArrayInputStream) {
				paramStreams.add((ByteArrayInputStream) value);
			}
			// if we find a map in the param, check if values are input streams
			if (value instanceof Map) {
				for (Object mapValue : ((Map) value).values()) {
					if (mapValue instanceof ByteArrayInputStream) {
						paramStreams.add((ByteArrayInputStream) mapValue);
					}
				}
			}
		}

		return paramStreams;
	}

	/**
	 * Reset grids inside parameters or inside maps in parameters
	 *
	 * @param params
	 *
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	protected void resetParamGrids(Map<String, Object> params) {
		// get input streams for reset from format, parameters, and grid object
		for (Object parameter : params.keySet()) {
			Object value = params.get(parameter);
			if (value instanceof DataGrid) {
				resetGrid((DataGrid) value, 0);
			}
			// if we find a map in the param, check if values are input streams
			// or data grids
			if (value instanceof Map) {
				for (Object mapValue : ((Map) value).values()) {
					if (mapValue instanceof DataGrid) {
						resetGrid((DataGrid) mapValue, 0);
					}
				}
			}
		}
	}

	/**
	 * Recursively reset all InputStreams in this grid and any sub-grids and
	 * reset them, also reset the index to the top.
	 * 
	 * @param grid
	 * 
	 * @throws IOException
	 */
	protected void resetGrid(DataGrid grid, int depth) {
		depth++;
		// or we have hit a max depth

		if (depth > MAX_DEPTH)
			return;

		// otherwise process this grid and its children
		grid.beforeTop();
		List<Map<String, Object>> rows = grid.getRows();

		for (Map<String, Object> row : rows) {
			for (Object value : row.values()) {
				if (value instanceof ByteArrayInputStream) {
					((ByteArrayInputStream) value).reset();
				} else if (value instanceof DataGrid) {
					resetGrid((DataGrid) value, depth);
				}
			}
		}
	}

	/**
	 * Creates a JasperPrint object from the data, which can be converted to a
	 * PDF.
	 * 
	 * @param format
	 * @param params
	 * @param reportData
	 *
	 * @return
	 */
	protected Object createJasperReport(InputStream format,
			Map<String, Object> params, ReportDataGrid reportData) {
		int version = 1;
		String engine = ((Report) job.getTool()).getEngineVersion();
		if (engine != null)
			version = Integer.valueOf(engine.substring(0, 1));
		try {
			if (version == 5) {
				net.sf.jasperreports5.engine.JasperReport jasperReport = (net.sf.jasperreports5.engine.JasperReport) net.sf.jasperreports5.engine.util.JRLoader
						.loadObject(format);

				jasperReport.setProperty(
						net.sf.jasperreports5.engine.fill.JRSubreportRunnerFactory.SUBREPORT_RUNNER_FACTORY,
						InlineThreadPoolSubreportRunnerFactory.class.getName());

				net.sf.jasperreports5.engine.DefaultJasperReportsContext defaultCtx = net.sf.jasperreports5.engine.DefaultJasperReportsContext
						.getInstance();
				net.sf.jasperreports5.engine.SimpleJasperReportsContext context = new net.sf.jasperreports5.engine.SimpleJasperReportsContext(
						defaultCtx);

				net.sf.jasperreports5.engine.fill.JRBaseFiller filler = net.sf.jasperreports5.engine.fill.JRFiller
						.createFiller(context, jasperReport);
				net.sf.jasperreports5.engine.JRPropertiesUtil props = filler
						.getPropertiesUtil();
				props.setProperty(
						net.sf.jasperreports5.engine.fill.JRSubreportRunnerFactory.SUBREPORT_RUNNER_FACTORY,
						InlineThreadPoolSubreportRunnerFactory.class.getName());

				// not sure if "master" would be different, but just in case:
				net.sf.jasperreports5.engine.fill.JRBaseFiller masterFiller = filler
						.getMasterFiller();
				props = masterFiller.getPropertiesUtil();
				props.setProperty(
						net.sf.jasperreports5.engine.fill.JRSubreportRunnerFactory.SUBREPORT_RUNNER_FACTORY,
						InlineThreadPoolSubreportRunnerFactory.class.getName());

				return filler.fill(params, reportData);
			} else if (version == 3) {
				return net.sf.jasperreports3.engine.JasperFillManager
						.fillReport(format, params, reportData);
			} else if (version == 1) {
				return net.sf.jasperreports5.engine.JasperFillManager
						.fillReport(format, params, reportData);
			}
		} catch (Exception e) {
			String msg = "Error filling report: "
					+ LoggerUtils.convertThrowableToString(e);
			logMessage(msg);
			throw new RuntimeException(msg);
		}

		return null;
	}

	/**
	 * Generate a PDF given a data grid, format, and params
	 * 
	 * @param reportData
	 * @param format
	 * @param params
	 *
	 * @return
	 *
	 * @throws JRException
	 * @throws net.sf.jasperreports3.engine.JRException
	 * @throws net.sf.jasperreports5.engine.JRException
	 * @throws IOException
	 */
	protected byte[] generatePdf(ReportDataGrid reportData, InputStream format,
			Map<String, Object> params)
			throws JRException, net.sf.jasperreports3.engine.JRException,
			net.sf.jasperreports5.engine.JRException, IOException {
		ByteArrayOutputStream pdfStream = new ByteArrayOutputStream();

		Object report = createJasperReport(format, params, reportData);
		if (report instanceof net.sf.jasperreports5.engine.JasperPrint) {
			net.sf.jasperreports5.engine.JasperExportManager
					.exportReportToPdfStream(
							(net.sf.jasperreports5.engine.JasperPrint) report,
							pdfStream);
		} else if (report instanceof net.sf.jasperreports3.engine.JasperPrint) {
			net.sf.jasperreports3.engine.JasperExportManager
					.exportReportToPdfStream(
							(net.sf.jasperreports3.engine.JasperPrint) report,
							pdfStream);
		} else if (report instanceof net.sf.jasperreports5.engine.JasperPrint) {
			net.sf.jasperreports5.engine.JasperExportManager
					.exportReportToPdfStream(
							(net.sf.jasperreports5.engine.JasperPrint) report,
							pdfStream);
		} else {
			String msg = "No matching Jasper Engine found in Aspen.";
			logMessage(msg);
			throw new RuntimeException(msg);
		}

		// reset the streams in this grid and reset the index to the top
		resetGrid(reportData, 0);
		resetParamGrids(params);

		// reset format and any parameters that are streams
		for (InputStream stream : streams) {
			stream.reset();
		}

		return pdfStream.toByteArray();
	}

	/**
	 * Create the file and attach it to the student as a Document object.
	 * 
	 * @param subject
	 *            of the document (student or staff)
	 * @param rawBytes
	 *
	 * @throws IOException
	 */
	protected void createDocument(Object bean, byte[] rawBytes)
			throws IOException {
		Tool m_report = job.getTool();
		if (bean != null) {
			// input stream from PDF bytes
			ByteArrayInputStream inFileStream = new ByteArrayInputStream(
					rawBytes);

			// write stream to file
			File file = FileAttachmentManager
					.createDestinationFile(organization, inFileStream);

			// retrieve school ID and person OID from the break column bean
			String schoolId = null;
			String personOid = null;

			if (breakColClass.getName().equals(Student.class.getName())) {
				personOid = ((Student) bean).getPersonOid();
				if (((Student) bean).getSchool() != null) {
					schoolId = ((Student) bean).getSchool().getSchoolId();
				}
			}
			if (breakColClass.getName().equals(Staff.class.getName())) {
				personOid = ((Staff) bean).getPersonOid();
				if (((Staff) bean).getSchool() != null) {
					schoolId = ((Staff) bean).getSchool().getSchoolId();
				}
			}
			if (breakColClass.getName().equals(Person.class.getName())) {
				personOid = ((Person) bean).getOid();
			}

			String reportName = null;
			PlainDate today = new PlainDate();
			// use report name
			if (StringUtils.isEmpty(customFilename)) {
				reportName = m_report.getName();
				if (reportName.length() > docNameField.getLength()) {
					reportName = reportName.substring(0,
							docNameField.getLength());
				}
			} else {
				String customReportName = customFilename;

				// remove ".pdf" from document name
				if (customReportName.toLowerCase().endsWith(PDF_EXTENSION)) {
					customReportName = StringUtils.replace(customReportName,
							PDF_EXTENSION, "");
				}

				// check document name for length
				if (customReportName.length() > docNameField.getLength()) {
					throw new RuntimeException(
							"Selected file name is too long for the document name field");
				} else {
					reportName = customReportName;
				}
			}

			String fileName = createFilename(bean, m_report, reportName);
			com.follett.fsc.core.k12.beans.Document document = (com.follett.fsc.core.k12.beans.Document) X2BaseBean
					.newInstance(com.follett.fsc.core.k12.beans.Document.class,
							broker.getPersistenceKey());
			document.setPersonOid(personOid);
			document.setName(reportName);
			document.setFilename(fileName);
			document.setFormatCode("PDF");

			if (!StringUtils.isEmpty(docTypeCode)) {
				document.setTypeCode(docTypeCode);
			}
			if (saveToDisk) {
				document.setBinaryFile(file);
			}
			if (saveToDatabase) {
				document.setDocument(rawBytes);
			}

			/*
			 * Upload Date user defined field
			 */
			if (uploadDateField != null) {
				document.setFieldValueByBeanPath(uploadDateField.getJavaName(),
						today.toString());
			}

			/*
			 * seth - Uploaded By User defined field
			 */

			if (uploadedByField != null) {
				userStaff = (Staff) user.getPerson().getStaff();

				if (userStaff != null) {
					userStaffPrintName = (String) userStaff
							.getFieldValueByAlias("cust-STF-short-name");
				}

				if (userStaffPrintName != null) {
					document.setFieldValueByBeanPath(
							uploadedByField.getJavaName(), userStaffPrintName);
				}
			}

			/*
			 * Report OID user defined field
			 */
			if (reportOidField != null) {
				document.setFieldValueByBeanPath(reportOidField.getJavaName(),
						m_report.getOid());
			}

			/*
			 * User OID user defined field
			 */
			if (userOidField != null) {
				document.setFieldValueByBeanPath(userOidField.getJavaName(),
						user.getOid());
			}

			/*
			 * School ID user defined field
			 */
			if (schoolIdField != null) {
				document.setFieldValueByBeanPath(schoolIdField.getJavaName(),
						schoolId);
			}

			try {
				Collection<ValidationError> errors = broker.saveBean(document);
				if (errors.size() > 0) {
					StringBuilder sb = new StringBuilder();
					Iterator<ValidationError> it = errors.iterator();
					sb.append(it.next().getCause());
					while (it.hasNext()) {
						sb.append(" | " + it.next().getCause());
					}
					// just log for now, should we stop execution?
					logMessage("Error saving document [" + fileName
							+ "] with errors: " + sb.toString());
				}
			} catch (Exception e) {
				AppGlobals.getLog()
						.severe("Error saving person document [" + fileName
								+ "]: "
								+ LoggerUtils.convertThrowableToString(e));
			}
		}
	}

	/**
	 * Creates a file name based on the report name, report ID, or attributes
	 * from the break column bean.
	 * 
	 * @param bean
	 * @param m_report
	 * @param reportName
	 *
	 * @return
	 */
	public String createFilename(Object bean, Tool m_report,
			String reportName) {
		String stateId = "";
		String localId = "";
		PlainDate date = new PlainDate();

		if (breakColClass.getName().equals(Student.class.getName())) {
			stateId = ((Student) bean).getStateId();
			localId = ((Student) bean).getLocalId();
		}
		if (breakColClass.getName().equals(Staff.class.getName())) {
			stateId = ((Staff) bean).getStateId();
			localId = ((Staff) bean).getLocalId();
		}
		if (breakColClass.getName().equals(Person.class.getName())) {
			localId = ((Person) bean).getPersonId();
		}

		/*
		 * By default, if the local ID exists, the file will be named:
		 * <localId>_<reportName>.pdf Otherwise, if the state ID exists, the
		 * file will be namned: <stateId>_<reportName>.pdf Otherwise it will be
		 * named: <reportName>.pdf
		 * 
		 * If a custom name is selected, it will be checked here for validity
		 * 
		 * Note: Files will be created every time this is run - duplicate names
		 * do not overwrite one another. Note: max filename length in Aspen 5.4
		 * is 60 characters Note: max filename length in Aspen 5.7 is 128
		 * characters
		 */

		String fileName = "";

		/*
		 * LS not want to have date or ID in fileName if
		 * (!org.apache.commons.lang3.StringUtils.isEmpty(localId)) { fileName =
		 * date + "_" + localId + "_" + reportName + PDF_EXTENSION; } else { if
		 * (!org.apache.commons.lang3.StringUtils.isEmpty(stateId)) { fileName =
		 * date + "_" + stateId + "_" + reportName + PDF_EXTENSION; } else {
		 * fileName = date + "_" + reportName + PDF_EXTENSION; } }
		 */
		// so here:
		fileName = reportName + PDF_EXTENSION;

		/*
		 * Final sanity checks on filename length to prevent truncation errors.
		 * 
		 * Do not change the file name below here, only above.
		 * 
		 * First try reportName.pdf If still too long, localId.pdf If still too
		 * long, reportId.pdf
		 */

		if (fileName.length() > fileNameField.getLength()) {
			fileName = date + "_" + reportName + PDF_EXTENSION;
		}

		if (fileName.length() > fileNameField.getLength()) {
			fileName = date + "_" + localId + PDF_EXTENSION;
		}

		if (fileName.length() > fileNameField.getLength()) {
			fileName = date + "_" + m_report.getId() + PDF_EXTENSION;
		}

		if (fileName.length() > fileNameField.getLength()) {
			fileName = date + PDF_EXTENSION;
		}

		return fileName;
	}

	/**
	 * Verify that the parameters passed to the constructor are populated, log
	 * errors if not.
	 * 
	 * @return boolean indicating whether we have the data we need to generate
	 *         documents
	 */
	protected boolean verifyParameters(ReportDataGrid grid, InputStream format,
			String breakCol) {
		boolean isValidParams = true;

		if (organization == null) {
			logMessage("organization is null.");
			isValidParams = false;
		}
		if (job == null) {
			logMessage("job is null.");
			isValidParams = false;
		}
		if (user == null) {
			logMessage("user is null.");
			isValidParams = false;
		}
		if (dictionary == null) {
			logMessage("dictionary is null.");
			isValidParams = false;
		}
		if (docNameField == null) {
			logMessage("document name fields is null.");
			isValidParams = false;
		}
		if (fileNameField == null) {
			logMessage("fileNameField is null.");
			isValidParams = false;
		}
		if (grid == null || grid.rowCount() == 0) {
			logMessage("source grid is null or empty.");
			isValidParams = false;
		}

		if (!grid.getColumns().contains(breakCol)) {
			logMessage("The break column [" + breakCol
					+ "] was not found in grid.");
			isValidParams = false;
		}

		if (format == null) {
			logMessage("report format is null.");
			isValidParams = false;
		}

		if (breakCol == null || "".equals(breakCol)) {
			logMessage("break column is null or empty.");
			isValidParams = false;
		}

		return isValidParams;
	}

	/**
	 * Log messages associated with document creation class.
	 * 
	 * @param msg
	 *            the String message to log
	 */
	protected void logMessage(String msg) {
		msg = LOG_LABEL + msg;
		Method logDetailMethod = null;
		try {
			logDetailMethod = JobLogManager.class.getMethod("logDetailMessage");
		} catch (NoSuchMethodException | SecurityException e) {
			// do nothing, this is fine.
		}
		if (logDetailMethod != null && job != null
				&& job.getJobLogManager() != null) {
			try {
				logDetailMethod.invoke(job.getJobLogManager(), Level.INFO, msg);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				logMessage("Error calling logDetailMessage(): "
						+ LoggerUtils.convertThrowableToString(e));
			}
		} else {
			AppGlobals.getLog().info(msg);
		}
	}
}