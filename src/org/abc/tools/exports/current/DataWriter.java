package org.abc.tools.exports.current;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.abc.util.BeanPathUtils;
import org.apache.ojb.broker.metadata.FieldHelper;
import org.apache.ojb.broker.query.Criteria;

import com.follett.cust.cub.ExportHelperCub.FileType;
import com.follett.cust.io.exporter.RowExporter;
import com.follett.cust.io.exporter.RowExporter.CellGroup;
import com.follett.fsc.core.framework.persistence.ColumnQuery;
import com.follett.fsc.core.framework.persistence.RowResultIteratorBuilder;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.ReportQueryIterator;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.business.X2Broker;
import com.x2dev.utils.ThreadUtils;

/**
 * This helps define and write a ColumnQuery to a data file.
 * <p>
 */
public class DataWriter {

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
	public static ColumnQuery createColumnQuery(PersistenceKey persistenceKey,
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
	 * Writes the results of a column query to a data file.
	 * 
	 * @param broker
	 * @param columnQuery
	 * @param fields
	 *            the fields to write (in order). This should be a subset of the
	 *            columns defined in the columnQuery.
	 * @param out
	 *            the OutputStream representing the file to write.
	 * @param fileExtension
	 *            the file extension such as "csv" or "xls"
	 * @param characterEncoding
	 *            the optional character encoding (such as "UTF-8")
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public void write(X2Broker broker, ColumnQuery columnQuery,
			List<BeanColumnPath> fields, OutputStream out,
			String fileExtension, String characterEncoding) throws Exception {
		FileType fileType = FileType.forFileExtension(fileExtension);

		List<String> columnNames = new ArrayList<>();
		for (int a = 0; a < fields.size(); a++) {
			String name = fields.get(a).getField(broker.getPersistenceKey())
					.getUserShortName();
			columnNames.add(name);
		}

		String header = null;
		try (RowExporter exporter = fileType.createRowExporter(out,
				characterEncoding, header)) {
			try (ReportQueryIterator iter = broker
					.getReportQueryIteratorByQuery(columnQuery)) {
				CellGroup group = new CellGroup(null, null, null,
						columnNames.toArray(new String[columnNames.size()]));
				Collection<String> fieldStrings = new HashSet<>(fields.size());
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

					List<String> cellValues = new ArrayList<>(reducedRow.size());
					for (int a = 0; a < reducedRow.size(); a++) {
						String str = toString(reducedRow.get(a));
						cellValues.add(str);
					}
					exporter.writeStrings(group, cellValues);
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

}
