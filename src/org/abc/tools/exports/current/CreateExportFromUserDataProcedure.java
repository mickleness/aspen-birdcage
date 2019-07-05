package org.abc.tools.exports.current;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.abc.tools.Tool;
import org.abc.tools.exports.AbstractCustomFileExtensionTool;
import org.abc.util.BeanPathUtils;
import org.apache.ojb.broker.metadata.FieldHelper;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.Query;

import com.follett.cust.cub.ExportHelperCub.FileType;
import com.follett.cust.io.Base64;
import com.follett.cust.io.exporter.RowExporter;
import com.follett.cust.io.exporter.RowExporter.CellGroup;
import com.follett.cust.io.html.Checklist;
import com.follett.cust.io.html.Checklist.NoteType;
import com.follett.cust.io.html.Checklist.SummaryItem;
import com.follett.cust.io.html.HtmlChecklist;
import com.follett.cust.io.html.HtmlImage;
import com.follett.cust.io.html.HtmlPage;
import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.ColumnQuery;
import com.follett.fsc.core.framework.persistence.RowResultIteratorBuilder;
import com.follett.fsc.core.framework.persistence.X2Criteria;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.FieldSet;
import com.follett.fsc.core.k12.beans.ImportExportDefinition;
import com.follett.fsc.core.k12.beans.ReportQueryIterator;
import com.follett.fsc.core.k12.beans.ToolSourceCode;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.business.ValidationError;
import com.follett.fsc.core.k12.web.ContextList;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.follett.fsc.core.k12.web.WebUtils;
import com.x2dev.utils.StringUtils;
import com.x2dev.utils.ThreadUtils;
import com.x2dev.utils.X2BaseException;
import com.x2dev.utils.types.PlainDate;

/**
 * This tool does one of two things, depending on the input parameters:
 * <ol>
 * <li>If the parameters "exportName" and "exportID" are defined: this will
 * create a new export. This new export will capture the current query/field set
 * and embed them as parameters in the new export. The next export will have
 * exactly the same source code as this export, but the input parameters will be
 * set up in such a way that it executes as the following:</li>
 * <li>If a query and field set are defined: this will export a table of data
 * reflecting that query/field set.</li>
 * </ol>
 * <p>
 * All exports support either XLS or CSV data.
 * 
 */
@Tool(id = "ABC-CREATE-EXPORT", name = "Create Export", input = "CreateExportFromUserdataProcedureInput.xml", type = "procedure", comment = "This creates a new Aspen export based on the current query and field set.", nodes = {
		"key=\"student.std.list\" org1-view=\"true\" school-view=\"true\"",
		"key=\"staff.staff.list\" org1-view=\"true\" school-view=\"true\"" })
public class CreateExportFromUserDataProcedure extends
		AbstractCustomFileExtensionTool {

	private static final long serialVersionUID = 1L;

	/**
	 * Serialize an object into a compressed base-64 encoded String.
	 * 
	 * @param object
	 *            the object to serialize.
	 * @return a String representation of the object that can be decoded by
	 *         calling {@link #deserializeBase64(String)}.
	 * 
	 * @throws Exception
	 */
	public static String serializeBase64(Serializable object) throws Exception {
		try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream()) {
			try (GZIPOutputStream zipOut = new GZIPOutputStream(byteOut)) {
				try (ObjectOutputStream objOut = new ObjectOutputStream(zipOut)) {
					objOut.writeObject(object);
				}
			}
			byte[] bytes = byteOut.toByteArray();
			return Base64.encode(bytes);
		}
	}

	/**
	 * Deserialize a compressed base-64 encoded String encoded with
	 * {@link #serializeBase64(Serializable)}.
	 * 
	 * @param str
	 *            the compressed based-64 encoded String.
	 * @return a deserialized Object.
	 * 
	 * @throws Exception
	 */
	public static Serializable deserializeBase64(String str) throws Exception {
		byte[] bytes = Base64.decode(str);
		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes)) {
			try (GZIPInputStream zipIn = new GZIPInputStream(byteIn)) {
				try (ObjectInputStream objIn = new ObjectInputStream(zipIn)) {
					return (Serializable) objIn.readObject();
				}
			}
		}
	}

	/**
	 * Create a ColumnQuery.
	 * 
	 * @param persistenceKey
	 * @param fields
	 *            the fields this query will retrieve
	 * @param baseClass
	 * @param criteria
	 * @param sortBy
	 *            a list of FieldHelpers indicating how to sort this query.
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static ColumnQuery createColumnQuery(PersistenceKey persistenceKey,
			List<BeanColumnPath> fields, Class baseClass, Criteria criteria,
			List<FieldHelper> sortBy) {
		if (fields.isEmpty())
			throw new RuntimeException(
					"Fields must be defined to construct the ColumnQuery");

		RowResultIteratorBuilder builder = new RowResultIteratorBuilder(
				persistenceKey, baseClass);
		builder.addColumns(fields);

		for (FieldHelper sortByHelper : sortBy) {
			BeanColumnPath bcp = (BeanColumnPath) BeanPathUtils.getBeanPath(
					baseClass, sortByHelper.name);
			builder.addOrderBy(bcp, sortByHelper.isAscending);
		}

		return builder.createColumnQuery(criteria);
	}

	/**
	 * This should resolve to a comma-separated list of fields to export, or an
	 * empty String if we should use the current FieldSet.
	 */
	protected static String PARAM_FIELDS = "fields";

	/**
	 * This should resolve to a base-64 encoded serialized ColumnQuery, or an
	 * empty String if we should use the current query.
	 */
	protected static String PARAM_QUERY = "query";

	/**
	 * This should resolve to the String "csv" or "xls".
	 */
	protected static String PARAM_FILE_EXTENSION = "fileExtension";

	/**
	 * This should resolve to the export name (if any)
	 */
	protected static String PARAM_NEW_EXPORT_NAME = "exportName";

	/**
	 * This should resolve to the export ID (if any)
	 */
	protected static String PARAM_NEW_EXPORT_ID = "exportID";

	/**
	 * A list of the fields to include in this export.
	 */
	@SuppressWarnings("rawtypes")
	List<BeanColumnPath> fields = new ArrayList<>();

	/**
	 * The ColumnQuery this export issues.
	 * <p>
	 * This must include all the fields in {@link #fields}, but it may also
	 * include additional fields (used for joins).
	 */
	ColumnQuery columnQuery;

	// these fields relate to creating a new ImportExportDefinition

	String newExportName, newExportID;
	boolean isCreateNewExport;

	@Override
	protected void run() throws Exception {
		try {
			if (isCreateNewExport) {
				createNewExport();
			} else {
				exportData();
			}
		} catch (Exception e) {
			addCustomErrorMessage(e.getMessage());
			throw e;
		}
	}

	/**
	 * Export a query as a file.
	 * <p>
	 * This pipes the data directly into the file one row at a time.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	protected void exportData() throws Exception {
		FileType fileType = FileType.forFileExtension(getFileExtension());

		List<String> columnNames = new ArrayList<>();
		for (int a = 0; a < fields.size(); a++) {
			String name = fields.get(a)
					.getField(getBroker().getPersistenceKey())
					.getUserShortName();
			columnNames.add(name);
		}

		// pipe data directly the OutputStream to save memory:
		try (OutputStream out = getResultHandler().getOutputStream()) {
			String header = null;
			try (RowExporter exporter = fileType.createRowExporter(out,
					getCharacterEncoding(), header)) {
				try (ReportQueryIterator iter = getBroker()
						.getReportQueryIteratorByQuery(columnQuery)) {
					CellGroup group = new CellGroup(null, null, null,
							columnNames.toArray(new String[columnNames.size()]));
					Collection<String> fieldStrings = new HashSet<>(
							fields.size());
					for (BeanColumnPath bcp : fields)
						fieldStrings.add(bcp.toString());
					String[] columns = columnQuery.getColumns();

					while (iter.hasNext()) {
						ThreadUtils.checkInterrupt();
						Object[] row = (Object[]) iter.next();

						// the row array may contain a few extra oids used for
						// joins we want to narrow this down to ONLY the values
						// we're outputting:
						List<Object> reducedRow = new ArrayList<>(row.length);
						for (int a = 0; a < columns.length; a++) {
							if (fieldStrings.contains(columns[a]))
								reducedRow.add(row[a]);
						}

						if (reducedRow.size() != columnNames.size())
							throw new IllegalStateException(
									"the number of incoming cell values didn't match the number of columns ("
											+ reducedRow.size() + "!="
											+ columnNames.size() + ")");

						List<String> cellValues = new ArrayList<>(
								reducedRow.size());
						for (int a = 0; a < reducedRow.size(); a++) {
							String str = toString(reducedRow.get(a));
							cellValues.add(str);
						}
						exporter.writeStrings(group, cellValues);
					}
				}
			}
		}
	}

	protected void createNewExport() throws Exception {
		try {
			getBroker().beginTransaction();
			boolean committedTransaction = false;
			try {
				// this line lets us overwrite existing IED's...
				// ... but that breaks a do-no-harm usability rule.
				// ImportExportDefinition ied = getPreexistingExport();

				ImportExportDefinition ied = null;
				ToolSourceCode tsc = null;
				if (ied == null) {
					ied = X2BaseBean.newInstance(ImportExportDefinition.class,
							getBroker().getPersistenceKey());
					tsc = X2BaseBean.newInstance(ToolSourceCode.class,
							getBroker().getPersistenceKey());
				} else {
					tsc = ied.getSourceCode();
				}

				String queryStr = serializeBase64(columnQuery);
				String fieldsStr = StringUtils
						.convertCollectionToDelimitedString(fields, ',');

				String exportFileExt = (String) getParameters().get(
						PARAM_FILE_EXTENSION);

				tsc.setInputDefinition("<tool-input>\n"
						+ "\t<input name=\""
						+ PARAM_QUERY
						+ "\" data-type=\"string\"  display-type=\"hidden\" default-value=\""
						+ queryStr
						+ "\" />\n"
						+ "\t<input name=\""
						+ PARAM_FIELDS
						+ "\" data-type=\"string\" display-type=\"text\" display-name=\"Fields\" default-value=\""
						+ fieldsStr
						+ "\" />\n"
						+ "\t<input name=\"fileExtension\" data-type=\"string\" display-type=\"select\" display-name=\"File Type\" default-value=\""
						+ exportFileExt + "\">\n"
						+ "\t\t<option value=\"xls\" display-name=\"xls\"/>\n"
						+ "\t\t<option value=\"csv\" display-name=\"csv\"/>\n"
						+ "\t</input>\n" + "</tool-input>");

				List<ValidationError> errors = new ArrayList<>();
				errors.addAll(getBroker().saveBean(tsc));
				tsc.setSourceCode(getJob().getTool().getSourceCode()
						.getSourceCode());
				tsc.setCompiledCode(getJob().getTool().getSourceCode()
						.getCompiledCode());

				ied.setOrganization1Oid(getOrganization().getOid());
				ied.setMenuGroup("Exports");
				ied.setSequenceNumber(0);
				ied.setWeight(1);
				ied.setType(ImportExportDefinition.TYPE_EXPORT);
				ied.setComment("This was created by " + getUser().getNameView()
						+ " using " + getJob().getTool().getId() + " on "
						+ (new PlainDate()) + ".");
				ied.setId(newExportID);
				ied.setName(newExportName);
				ied.setJarPluginId(getJob().getTool().getJarPluginId());
				ied.setJarPluginPath(getJob().getTool().getJarPluginPath());

				ied.setSourceCodeOid(tsc.getOid());
				errors.addAll(getBroker().saveBean(ied));

				getBroker().commitTransaction();
				logToolMessage(Level.INFO, "commit " + ied.getOid(), false);
				committedTransaction = true;

				publishNewExportHTMLResults(errors, tsc, ied);
			} finally {
				if (!committedTransaction) {
					logToolMessage(Level.INFO, "rollback", false);
					getBroker().rollbackTransaction();
				}
			}
		} catch (Exception e) {
			addCustomErrorMessage(e.getMessage());
			throw e;
		}
	}

	/**
	 * Returns the preexisting ImportExportDefinition with the requested
	 * name/ID, or null if no such export exists.
	 */
	private ImportExportDefinition getPreexistingExport() {
		X2Criteria criteria = new X2Criteria();
		criteria.addEqualTo(ImportExportDefinition.COL_NAME, newExportName);
		criteria.addEqualTo(ImportExportDefinition.COL_ID, newExportID);
		BeanQuery q = new BeanQuery(ImportExportDefinition.class, criteria);
		return (ImportExportDefinition) getBroker().getBeanByQuery(q);
	}

	/**
	 * Publish HTML describing this procedure's results (either as a success or
	 * as a failure).
	 * 
	 * @param errors
	 *            any ValidationErrors that occurred that need relaying
	 * @param tsc
	 *            the ToolSourceCode that was created
	 * @param ied
	 *            the ImportExportDefinition that was created.
	 * 
	 * @throws IOException
	 */
	private void publishNewExportHTMLResults(
			final List<ValidationError> errors, final ToolSourceCode tsc,
			final ImportExportDefinition ied) throws IOException {

		Checklist checklist = new Checklist();
		SummaryItem summaryItem = checklist.addSummaryItem(0, "summary");
		if (errors.isEmpty()) {
			summaryItem.setTitle("Export Successfully Created");
			summaryItem.setImage(HtmlImage.GENERIC_PASS_48);
			summaryItem.addNote(NoteType.PLAIN, "The export \"" + ied.getName()
					+ "\" (ID \"" + ied.getId()
					+ "\") was successfully created.", true, true);

			summaryItem
					.addNote(
							NoteType.WARNING,
							"This new export may not show up in all the right places until you log out and log back in.",
							true, true);
		} else {
			summaryItem.setTitle("No Export Created");
			summaryItem.setImage(HtmlImage.GENERIC_FAIL_48);
			summaryItem
					.addNote(
							NoteType.PLAIN,
							"The export \""
									+ ied.getName()
									+ "\" (ID \""
									+ ied.getId()
									+ "\") could not be created because of the following errors:",
							true, true);
			for (ValidationError e : errors) {
				String msg;
				try {
					msg = WebUtils.getMessage(e, getBroker()
							.getPersistenceKey());
				} catch (Exception e2) {
					// WebUtils threw a ClassCastException
					msg = e.toString();
				}
				summaryItem.addNote(NoteType.ERROR, msg, true, true);
			}
		}

		try (OutputStream out = getResultHandler().getOutputStream()) {
			try (HtmlPage page = new HtmlPage(out, "Results")) {
				page.add(new HtmlChecklist(checklist, false));
			}

		}
	}

	/**
	 * Convert an Object from a query into a String for the exported file.
	 * 
	 * @param value
	 *            a value, including null, PlainDates, Integers, etc.
	 * @return the String to put in the exported file.
	 */
	protected String toString(Object value) {
		if (value == null)
			return "";
		return value.toString();
	}

	@Override
	protected void saveState(UserDataContainer userData) throws X2BaseException {
		super.saveState(userData);
		try {
			String queryStr = (String) getParameters().get(PARAM_QUERY);
			String fieldsStr = (String) getParameters().get(PARAM_FIELDS);

			if (StringUtils.isEmpty(queryStr) && StringUtils.isEmpty(fieldsStr)) {
				initializeUsingCurrentState(userData);
				isCreateNewExport = true;
				setFileExtension("html");
			} else if ((!StringUtils.isEmpty(queryStr))
					&& (!StringUtils.isEmpty(fieldsStr))) {
				initialize(queryStr, fieldsStr);
				isCreateNewExport = false;
				setFileExtension((String) getParameters().get(
						PARAM_FILE_EXTENSION));
			} else {
				throw new RuntimeException("The parameters \"" + PARAM_FIELDS
						+ "\" and \"" + PARAM_QUERY
						+ "\" should either both be defined or both be empty.");
			}

			newExportName = (String) getParameters().get(PARAM_NEW_EXPORT_NAME);
			newExportID = (String) getParameters().get(PARAM_NEW_EXPORT_ID);
		} catch (X2BaseException e) {
			throw e;
		} catch (Exception e) {
			throw new X2BaseException(e);
		}
	}

	/**
	 * Initialize the {@link #fields} and {@link #query} fields based on the
	 * UserDataContainer.
	 * 
	 * @param userData
	 *            the UserData used to identify the current fields/query.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void initializeUsingCurrentState(UserDataContainer userData) {
		ContextList currentList = userData.getCurrentList();

		String fieldSetOid = userData.getCurrentList().getSelectedFieldSetOid();
		FieldSet fieldSet = (FieldSet) getBroker().getBeanByOid(FieldSet.class,
				fieldSetOid);
		fields = (List) BeanPathUtils.getBeanPaths(fieldSet);

		Query q = currentList.getQuery();
		columnQuery = createColumnQuery(getBroker().getPersistenceKey(),
				fields, q.getBaseClass(), q.getCriteria(), q.getOrderBy());
	}

	/**
	 * Initialize the {@link #fields} and {@link #query} fields based on the
	 * given parameters.
	 * 
	 * @param queryParam
	 *            a base-64 encoded serialized copy of a ColumnQuery
	 * @param fieldsParam
	 *            a comma-separated list of fields
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void initialize(String queryParam, String fieldsParam)
			throws Exception {
		ColumnQuery q = (ColumnQuery) deserializeBase64(queryParam);

		String[] terms = fieldsParam.split(",");
		for (String term : terms) {
			fields.add((BeanColumnPath) BeanPathUtils.getBeanPath(
					q.getBaseClass(), term));
		}

		// re-initialize the ColumnQuery now that we know the exact fields the
		// user wants in this pass:
		columnQuery = createColumnQuery(getBroker().getPersistenceKey(),
				fields, q.getBaseClass(), q.getCriteria(), q.getOrderBy());
	}
}
