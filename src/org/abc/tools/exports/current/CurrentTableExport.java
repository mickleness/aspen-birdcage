package org.abc.tools.exports.current;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.abc.tools.Tool;
import org.abc.tools.exports.AbstractCustomFileExtensionTool;
import org.abc.util.BeanPathUtils;

import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.ColumnQuery;
import com.follett.fsc.core.k12.beans.FieldSet;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.web.ContextList;
import com.follett.fsc.core.k12.web.UserDataContainer;
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

	@Override
	protected void run() throws Exception {
		try {
			try (OutputStream out = getResultHandler().getOutputStream()) {
				DataWriter writer = new DataWriter();
				writer.write(getBroker(), columnQuery, fields, out,
						getFileExtension(), getCharacterEncoding());
			}
		} catch (Exception e) {
			addCustomErrorMessage(e.getMessage());
			throw e;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void saveState(UserDataContainer userData) throws X2BaseException {
		try {
			super.saveState(userData);

			ContextList currentList = userData.getCurrentList();

			String fieldSetOid = currentList.getSelectedFieldSetOid();
			FieldSet fieldSet = (FieldSet) getBroker().getBeanByOid(
					FieldSet.class, fieldSetOid);
			fields = (List) BeanPathUtils.getBeanPaths(fieldSet);

			BeanQuery q = currentList.getQuery();
			columnQuery = DataWriter.createColumnQuery(getBroker()
					.getPersistenceKey(), fields, q.getBaseClass(), q
					.getCriteria(), q.getOrderBy());

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
