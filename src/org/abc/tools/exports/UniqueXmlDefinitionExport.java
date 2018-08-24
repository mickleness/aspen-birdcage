package org.abc.tools.exports;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.follett.fsc.core.k12.tools.exports.XmlDefinitionExport;
import com.x2dev.utils.DataGrid;


/**
 * Attach this source code to an XML export to remove redundant rows from your
 * data.
 */
public class UniqueXmlDefinitionExport extends XmlDefinitionExport {
	private static final long serialVersionUID = 1L;

	/**
	 * This removes redundant rows from export data.
	 */
	@Override
	protected void afterGatherData(DataGrid dataGrid) {
		List<Map<String, Object>> allRows = dataGrid.getRows();
		Iterator<Map<String, Object>> rowIter = allRows.iterator();
		Map<String, Object> lastRow = null;
		while (rowIter.hasNext()) {
			Map<String, Object> row = rowIter.next();
			if (row.equals(lastRow)) {
				rowIter.remove();
			}
			lastRow = row;
		}
	}
}