package org.abc.tools.exports.current;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.abc.tools.Tool;
import org.abc.tools.exports.AbstractCustomFileExtensionTool;
import org.abc.util.BeanPathUtils;

import com.follett.cust.cub.ExportHelperCub.FileType;
import com.follett.cust.io.exporter.RowExporter;
import com.follett.cust.io.exporter.RowExporter.CellGroup;
import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.k12.beans.FieldSet;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.business.dictionary.DataDictionaryField;
import com.follett.fsc.core.k12.web.ContextList;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.utils.KeyValuePair;
import com.x2dev.utils.ThreadUtils;
import com.x2dev.utils.X2BaseException;

/**
 * This exports the table the user is currently viewing (using the same query
 * and field set) as a CSV or XLS file.
 * <p>
 * Declaring this as a type "procedure" appears to help its visibility in Aspen.
 */
@Tool(id = "ABC-CURRENT-TBL-EXP", name = "Export to File", input = "CurrentTableExportInput.xml", type = "procedure", comment = "This exports the current table data to a CSV or XLS file.", nodes = {
		"key=\"student.std.list\" org1-view=\"true\" school-view=\"true\"",
		"key=\"staff.staff.list\" org1-view=\"true\" school-view=\"true\"" })
public class CurrentTableExport extends AbstractCustomFileExtensionTool {
	private static final long serialVersionUID = 1L;

	/**
	 * This should resolve to the String "csv" or "xls".
	 */
	protected static String PARAM_FILE_EXTENSION = "fileExtension";

	/**
	 * The BeanQuery this export issues.
	 */
	BeanQuery beanQuery;

	/**
	 * A list of column java names and header names. So for a student the first
	 * value might be "nameView" (which is how we retrieve it from the bean),
	 * and the second value would be "Name" (which is how the column in the
	 * export is labeled).
	 */
	List<KeyValuePair<String, String>> columns = new ArrayList<>();

	@Override
	protected void run() throws Exception {
		try {
			FileType fileType = FileType.forFileExtension(getFileExtension());

			List<String> columnNames = new ArrayList<>();
			for (int a = 0; a < columns.size(); a++) {
				columnNames.add(columns.get(a).getValue());
			}

			// pipe data directly the OutputStream to save memory:
			try (OutputStream out = getResultHandler().getOutputStream()) {
				String header = null;
				try (RowExporter exporter = fileType.createRowExporter(out,
						getCharacterEncoding(), header)) {
					try (QueryIterator iter = getBroker().getIteratorByQuery(
							beanQuery)) {
						CellGroup group = new CellGroup(null, null, null,
								columnNames.toArray(new String[columnNames
										.size()]));

						while (iter.hasNext()) {
							ThreadUtils.checkInterrupt();
							X2BaseBean bean = (X2BaseBean) iter.next();
							List<Object> row = new ArrayList<>();
							for (int a = 0; a < columns.size(); a++) {
								row.add(bean.getFieldValueByBeanPath(columns
										.get(a).getKey()));
							}

							List<String> cellValues = new ArrayList<>(
									row.size());
							for (int a = 0; a < row.size(); a++) {
								String str = toString(row.get(a));
								cellValues.add(str);
							}
							exporter.writeStrings(group, cellValues);
						}
					}
				}
			}
		} catch (Exception e) {
			addCustomErrorMessage(e.getMessage());
			throw e;
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void saveState(UserDataContainer userData) throws X2BaseException {
		try {
			super.saveState(userData);

			ContextList currentList = userData.getCurrentList();

			String fieldSetOid = userData.getCurrentList()
					.getSelectedFieldSetOid();
			FieldSet fieldSet = (FieldSet) getBroker().getBeanByOid(
					FieldSet.class, fieldSetOid);
			List<BeanColumnPath> bcps = (List) BeanPathUtils
					.getBeanPaths(fieldSet);

			for (BeanColumnPath bcp : bcps) {
				DataDictionaryField ddf = bcp.getField(getBroker()
						.getPersistenceKey());
				KeyValuePair<String, String> column = new KeyValuePair<>(
						bcp.toString(), ddf.getUserShortName());
				columns.add(column);
			}

			beanQuery = currentList.getQuery();

			String ext = (String) getParameters().get(PARAM_FILE_EXTENSION);
			if (ext == null)
				ext = "csv";
			setFileExtension(ext);
		} catch (X2BaseException e) {
			throw e;
		} catch (Exception e) {
			throw new X2BaseException(e);
		}
	}
}
