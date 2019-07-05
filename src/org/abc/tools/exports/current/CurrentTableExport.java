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
import java.util.Random;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.ojb.broker.metadata.FieldHelper;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.Query;

import com.follett.cust.cub.ExportHelperCub.FileType;
import com.follett.cust.io.Base64;
import com.follett.cust.io.IndentedPrintStream;
import com.follett.cust.io.exporter.RowExporter;
import com.follett.cust.io.exporter.RowExporter.CellGroup;
import com.follett.cust.io.html.HtmlComponent;
import com.follett.cust.io.html.HtmlEncoder;
import com.follett.cust.io.html.HtmlPage;
import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.ColumnQuery;
import com.follett.fsc.core.framework.persistence.RowResultIteratorBuilder;
import com.follett.fsc.core.framework.persistence.X2Criteria;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.FieldSet;
import com.follett.fsc.core.k12.beans.FieldSetMember;
import com.follett.fsc.core.k12.beans.ImportExportDefinition;
import com.follett.fsc.core.k12.beans.ReportQueryIterator;
import com.follett.fsc.core.k12.beans.ToolSourceCode;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.beans.path.BeanPath;
import com.follett.fsc.core.k12.beans.path.BeanPathValidationException;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.ModelProperty;
import com.follett.fsc.core.k12.business.ValidationError;
import com.follett.fsc.core.k12.tools.ToolJavaSource;
import com.follett.fsc.core.k12.web.ContextList;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.utils.StringUtils;
import com.x2dev.utils.ThreadUtils;
import com.x2dev.utils.X2BaseException;
import com.x2dev.utils.types.PlainDate;

/**
 * This exports a ColumnQuery that can come from either the current user's view
 * or from a serialized parameter.
 */
public class CurrentTableExport extends ToolJavaSource {

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
	 * This converts a path from an oid-based based to a java name.
	 * <p>
	 * This is copied and pasted from BeanPath.getBeanPath(), but it is enhanced
	 * to better support model/oid-based input.
	 * 
	 * @param beanType
	 *            the type of bean this path originates from, such as
	 *            SisStudent.class
	 * @param beanPath
	 *            a path such as "relStdPsnOid.relPsnAdrMail.adrCity"
	 * @return a BeanColumnPath representing a path like
	 *         "person.mailingAddress.city"
	 * @throws BeanPathValidationException
	 */
	public static <B extends X2BaseBean> BeanPath<B, ?, ?> getBeanPath(
			Class<B> beanType, final String beanPath)
			throws BeanPathValidationException {
		if (beanType == null) {
			throw new NullPointerException("The bean type is null.");
		}
		String[] terms = beanPath.split("\\"
				+ Character.toString(ModelProperty.PATH_DELIMITER));
		BeanTablePath<B, ?, ?> table = BeanTablePath.getTable(beanType);
		if (table == null) {
			throw new NullPointerException(
					"There is no BeanTablePath available for "
							+ beanType.getName());
		}
		for (int a = 0; a < terms.length - 1; a++) {
			BeanTablePath<B, ?, ?> nextTable = table.getTable(terms[a]);
			if (nextTable == null) {
				findRelatedTable: for (BeanTablePath rel : table.getTables()) {
					if (rel.getRelationshipOid().equals(terms[a])) {
						nextTable = rel;
						break findRelatedTable;
					}
				}
			}
			if (nextTable == null) {
				throw new RuntimeException("Unable to find field \"" + beanPath
						+ "\" on table "
						+ BeanTablePath.getTable(beanType).getDatabaseName()
						+ " (failed on \"" + terms[a] + "\"");
			}
			table = nextTable;
		}
		String lastTerm = terms[terms.length - 1];
		BeanPath<B, ?, ?> lastPath = table.getTable(lastTerm);
		if (lastPath == null) {
			lastPath = table.getColumn(lastTerm);
		}
		if (lastPath == null) {
			throw new RuntimeException("Unable to find field \"" + beanPath
					+ "\" on table "
					+ BeanTablePath.getTable(beanType).getDatabaseName());
		}
		return lastPath;
	}

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
			BeanColumnPath bcp = (BeanColumnPath) getBeanPath(baseClass,
					sortByHelper.name);
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
	List<BeanColumnPath> fields = new ArrayList<>();

	/**
	 * The ColumnQuery this export issues.
	 * <p>
	 * This must include all the fields in {@link #fields}, but it may also
	 * include additional fields (used for joins).
	 */
	ColumnQuery columnQuery;

	String newExportName, newExportID;
	boolean isCreateNewExport;

	String customFileName, fileExtension;

	@Override
	protected void run() throws Exception {
		if (isCreateNewExport) {
			createNewExport();
		} else {
			exportData();
		}
	}

	protected void createNewExport() throws Exception {
		getBroker().beginTransaction();
		boolean committedTransaction = false;
		try {
			ImportExportDefinition ied = getExport();
			ToolSourceCode tsc = null;
			if (ied == null) {
				ied = X2BaseBean.newInstance(ImportExportDefinition.class,
						getBroker().getPersistenceKey());
				tsc = X2BaseBean.newInstance(ToolSourceCode.class, getBroker()
						.getPersistenceKey());
			} else {
				tsc = ied.getSourceCode();
			}

			String queryStr = serializeBase64(columnQuery);
			String fieldsStr = StringUtils.convertCollectionToDelimitedString(
					fields, ',');

			// the field "fileExtension" is now "html", because this tool will
			// produce an HTML summary. Right now we want the real target export
			// format:
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
			ied.setComment("");
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
	}

	private ImportExportDefinition getExport() {
		X2Criteria criteria = new X2Criteria();
		criteria.addEqualTo(ImportExportDefinition.COL_NAME, newExportName);
		criteria.addEqualTo(ImportExportDefinition.COL_ID, newExportID);
		BeanQuery q = new BeanQuery(ImportExportDefinition.class, criteria);
		return (ImportExportDefinition) getBroker().getBeanByQuery(q);
	}

	private void publishNewExportHTMLResults(
			final List<ValidationError> errors, final ToolSourceCode tsc,
			final ImportExportDefinition ied) throws IOException {
		try (OutputStream out = getResultHandler().getOutputStream()) {
			try (HtmlPage page = new HtmlPage(out, "Results")) {
				page.add(new HtmlComponent() {

					@Override
					public void installDependencies(HtmlPage page) {
						// intentionally empty
					}

					@Override
					public void writeHtml(HtmlPage page, String id,
							IndentedPrintStream stream) {
						if (errors.isEmpty()) {
							stream.println("Successfully saved export "
									+ ied.getName() + " (" + ied.getId()
									+ "). " + ied.getOid() + " / "
									+ tsc.getOid());
						} else {
							stream.println("Errors occurred saving the new export \""
									+ ied.getName()
									+ "\" (no changes were saved):\n"
									+ HtmlEncoder.encode(errors.toString()));
						}
					}

				});
			}

		}
	}

	protected void exportData() throws Exception {
		FileType fileType = FileType.forFileExtension(fileExtension);

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
						@SuppressWarnings("unchecked")
						Object[] row = (Object[]) iter.next();

						// the row array may contain a few extra oids used for
						// joins
						// we want to narrow this down to ONLY the values we're
						// outputting:
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
			} else if ((!StringUtils.isEmpty(queryStr))
					&& (!StringUtils.isEmpty(fieldsStr))) {
				initialize(queryStr, fieldsStr);
			} else {
				throw new RuntimeException("The parameters \"" + PARAM_FIELDS
						+ "\" and \"" + PARAM_QUERY
						+ "\" should either both be defined or both be empty.");
			}

			fileExtension = (String) getParameters().get(PARAM_FILE_EXTENSION);
			if (fileExtension == null)
				fileExtension = "csv";

			newExportName = (String) getParameters().get(PARAM_NEW_EXPORT_NAME);
			newExportID = (String) getParameters().get(PARAM_NEW_EXPORT_ID);
			if (StringUtils.isEmpty(newExportName)
					&& StringUtils.isEmpty(newExportID)) {
				isCreateNewExport = false;
			} else if (!(StringUtils.isEmpty(newExportName))
					&& !(StringUtils.isEmpty(newExportID))) {
				isCreateNewExport = true;
				// we're going to make a page describing the new tool
				fileExtension = "html";
			} else if (StringUtils.isEmpty(newExportName)) {
				throw new RuntimeException(
						"If the export ID is defined the export name must be defined too.");
			} else if (StringUtils.isEmpty(newExportID)) {
				throw new RuntimeException(
						"If the export name is defined the export ID must be defined too.");
			}
		} catch (X2BaseException e) {
			throw e;
		} catch (Exception e) {
			throw new X2BaseException(e);
		}
	}

	protected void initialize() throws X2BaseException {
		super.initialize();
		initializeFileInfo();
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
		if (customFileName == null && fileExtension != null) {
			FileType f = FileType.forFileExtension(fileExtension);
			Random random = new Random();
			customFileName = "export" + random.nextInt(1000) + "."
					+ f.fileExtension;
			getJob().getInput().setFormat(f.toolInputType);
		}
	}

	/**
	 * Initialize the {@link #fields} and {@link #query} fields based on the
	 * UserDataContainer.
	 * 
	 * @param userData
	 *            the UserData used to identify the current fields/query.
	 */
	protected void initializeUsingCurrentState(UserDataContainer userData) {
		ContextList currentList = userData.getCurrentList();

		String fieldSetOid = userData.getCurrentList().getSelectedFieldSetOid();
		FieldSet fieldSet = (FieldSet) getBroker().getBeanByOid(FieldSet.class,
				fieldSetOid);
		for (FieldSetMember fsf : fieldSet.getMembers()) {
			String oid = (!StringUtils.isEmpty(fsf.getRelation()) ? fsf
					.getRelation() + "." : "")
					+ fsf.getObjectOid();

			// these sometimes including trailing spaces. Not sure why
			oid = oid.trim();

			BeanColumnPath bcp = (BeanColumnPath) getBeanPath(currentList
					.getQuery().getBaseClass(), oid);
			fields.add(bcp);
		}

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
	protected void initialize(String queryParam, String fieldsParam)
			throws Exception {
		ColumnQuery q = (ColumnQuery) deserializeBase64(queryParam);

		String[] terms = fieldsParam.split(",");
		for (String term : terms) {
			fields.add((BeanColumnPath) getBeanPath(q.getBaseClass(), term));
		}

		// re-initialize the ColumnQuery now that we know the exact fields the
		// user wants in this pass:
		columnQuery = createColumnQuery(getBroker().getPersistenceKey(),
				fields, q.getBaseClass(), q.getCriteria(), q.getOrderBy());
	}
}
