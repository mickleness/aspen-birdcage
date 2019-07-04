package org.abc.tools.exports.current;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.ojb.broker.query.Criteria;

import com.follett.cust.cub.ExportHelperCub.FileType;
import com.follett.cust.io.Base64;
import com.follett.cust.io.exporter.RowExporter;
import com.follett.cust.io.exporter.RowExporter.CellGroup;
import com.follett.fsc.core.framework.persistence.ColumnQuery;
import com.follett.fsc.core.framework.persistence.RowResultIteratorBuilder;
import com.follett.fsc.core.k12.beans.FieldSet;
import com.follett.fsc.core.k12.beans.FieldSetMember;
import com.follett.fsc.core.k12.beans.ReportQueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.beans.path.BeanPath;
import com.follett.fsc.core.k12.beans.path.BeanPathValidationException;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.ModelProperty;
import com.follett.fsc.core.k12.tools.ToolJavaSource;
import com.follett.fsc.core.k12.web.ContextList;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.utils.StringUtils;
import com.x2dev.utils.ThreadUtils;
import com.x2dev.utils.X2BaseException;

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
	public <B extends X2BaseBean> BeanPath<B, ?, ?> getBeanPath(
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
				throw new RuntimeException("Unabled to find term " + a + " ("
						+ terms[a] + ")");
			}
			table = nextTable;
		}
		String lastTerm = terms[terms.length - 1];
		BeanPath<B, ?, ?> lastPath = table.getTable(lastTerm);
		if (lastPath == null) {
			lastPath = table.getColumn(lastTerm);
		}
		if (lastPath == null) {
			throw new RuntimeException("Unabled to find term " + lastTerm);
		}
		return lastPath;
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

	@Override
	protected void run() throws Exception {

		String fileExtension = (String) getParameters().get(
				PARAM_FILE_EXTENSION);
		FileType fileType = FileType.forFileExtension(fileExtension);
		if (fileType == null) {
			if (StringUtils.isEmpty(fileExtension)) {
				fileType = FileType.CSV;
			} else {
				throw new IllegalArgumentException("Unsupported file type \""
						+ fileType + "\"");
			}
		}

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
	protected void initializeUsingCurrentState(UserDataContainer userData) {
		ContextList currentList = userData.getCurrentList();
		RowResultIteratorBuilder builder = new RowResultIteratorBuilder(
				getBroker().getPersistenceKey(), currentList.getQuery()
						.getBaseClass());

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
			builder.addColumn(bcp);
		}

		// currentList.getQuery() returns a BeanQuery,
		// but we want to use a ColumnQuery instead.
		Criteria criteria = currentList.getQuery().getCriteria();
		columnQuery = builder.createColumnQuery(criteria);
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
		columnQuery = (ColumnQuery) deserializeBase64(queryParam);

		String[] terms = fieldsParam.split(",");
		for (String term : terms) {
			fields.add((BeanColumnPath) getBeanPath(columnQuery.getBaseClass(),
					term));
		}
	}
}
