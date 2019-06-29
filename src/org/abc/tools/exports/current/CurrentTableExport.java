package org.abc.tools.exports.current;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.ojb.broker.query.Query;

import com.follett.cust.cub.ExportHelperCub;
import com.follett.cust.cub.ExportHelperCub.FileType;
import com.follett.cust.io.Base64;
import com.follett.cust.io.exporter.RowExporter;
import com.follett.cust.io.exporter.RowExporter.CellGroup;
import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.k12.beans.BeanManager.PersistenceKey;
import com.follett.fsc.core.k12.beans.FieldSet;
import com.follett.fsc.core.k12.beans.FieldSetMember;
import com.follett.fsc.core.k12.beans.ImportExportDefinition;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.beans.path.BeanPath;
import com.follett.fsc.core.k12.beans.path.BeanPathValidationException;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.ModelProperty;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.tools.ResultHandler;
import com.follett.fsc.core.k12.tools.ToolJavaSource;
import com.follett.fsc.core.k12.web.ContextList;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.utils.StringUtils;
import com.x2dev.utils.ThreadUtils;

public class CurrentTableExport extends ToolJavaSource {

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
		int charsRead = 0;
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
			charsRead += terms[a].length() + 1;
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

	StreamingTableWriterRunnable tableWriterRunnable;

	@Override
	protected void run() throws Exception {
		tableWriterRunnable.run(getBroker(), getResultHandler());
	}

	@Override
	protected void saveState(UserDataContainer userData) {
		ContextList currentList = userData.getCurrentList();

		BeanQuery query = currentList.getQuery();
		// TODO allow fileextension to change in different invocations
		String fileExtension = "csv";
		FileType fileType = FileType.forFileExtension(fileExtension);

		String fieldSetOid = userData.getCurrentList().getSelectedFieldSetOid();
		FieldSet fieldSet = (FieldSet) getBroker().getBeanByOid(FieldSet.class,
				fieldSetOid);
		List<BeanColumnPath> fields = new ArrayList<>();
		for (FieldSetMember fsf : fieldSet.getMembers()) {
			String oid = (!StringUtils.isEmpty(fsf.getRelation()) ? fsf
					.getRelation() + "." : "")
					+ fsf.getObjectOid();
			oid = oid.trim();
			logToolMessage(Level.INFO, oid, false);
			BeanColumnPath bcp = (BeanColumnPath) getBeanPath(
					query.getBaseClass(), oid);
			fields.add(bcp);
		}

		StreamingTableWriter<?> tableWriter = new DefaultTableWriter(fileType,
				fields, getCharacterEncoding(), getBroker().getPersistenceKey());

		tableWriterRunnable = new StreamingTableWriterRunnable(tableWriter,
				query);
	}
}

class ExecuteToolRunnable extends ToolJavaSource {
	private static final long serialVersionUID = 1L;

	@Override
	protected void run() throws Exception {
		ImportExportDefinition ied = (ImportExportDefinition) getJob()
				.getTool();
		ToolRunnable runnable = (ToolRunnable) CurrentTableExport
				.deserializeBase64(ied.getDefinition());
		runnable.run(getBroker(), getResultHandler());
	}
}

class StreamingTableWriterRunnable<T extends X2BaseBean> implements
		ToolRunnable {
	private static final long serialVersionUID = 1L;

	private StreamingTableWriter<T> tableWriter;

	private Query query;

	public StreamingTableWriterRunnable(StreamingTableWriter<T> tableWriter,
			Query query) {
		setTableWriter(tableWriter);
		setQuery(query);
	}

	public StreamingTableWriter<T> getTableWriter() {
		return tableWriter;
	}

	public void setTableWriter(StreamingTableWriter<T> tableWriter) {
		Objects.requireNonNull(tableWriter);
		this.tableWriter = tableWriter;
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		Objects.requireNonNull(query);
		this.query = query;
	}

	@Override
	public void run(X2Broker broker, ResultHandler resultHandler)
			throws Exception {
		try (OutputStream out = resultHandler.getOutputStream()) {
			try (QueryIterator iter = broker.getIteratorByQuery(query)) {
				tableWriter.write(out, iter);
			}
		}
	}
}

interface ToolRunnable extends Serializable {
	public void run(X2Broker broker, ResultHandler resultHandler)
			throws Exception;
}

class DefaultTableWriter<T extends X2BaseBean> extends StreamingTableWriter<T> {
	private static final long serialVersionUID = 1L;

	private List<BeanColumnPath<T, ?, ?>> fields;

	public DefaultTableWriter(FileType fileType,
			List<BeanColumnPath<T, ?, ?>> fields, String characterEncoding,
			PersistenceKey persistenceKey) {
		super(fileType, new ArrayList<String>(), characterEncoding);
		setFields(fields);
		List<String> columnNames = new ArrayList<>(fields.size());
		for (int a = 0; a < fields.size(); a++) {
			String name = fields.get(a).getField(persistenceKey)
					.getUserLongName();
			columnNames.add(name);
		}
		setColumnNames(columnNames);
	}

	public List<BeanColumnPath<T, ?, ?>> getFields() {
		return Collections.unmodifiableList(fields);
	}

	public void setFields(List<BeanColumnPath<T, ?, ?>> fields) {
		this.fields = fields;
	}

	@Override
	protected List<String> getRowValues(T bean) {
		List<String> returnValue = new ArrayList<>(getFields().size());
		for (int a = 0; a < getFields().size(); a++) {
			Object value = bean.getFieldValueByBeanPath(fields.get(a)
					.toString());
			returnValue.add(toString(value));
		}
		return returnValue;
	}

	protected String toString(Object value) {
		if (value == null)
			return "";
		return value.toString();
	}

}

abstract class StreamingTableWriter<T> implements Serializable {
	private static final long serialVersionUID = 1L;

	private List<String> columnNames;
	private ExportHelperCub.FileType fileType;
	private String characterEncoding;

	public StreamingTableWriter(ExportHelperCub.FileType fileType,
			List<String> columnNames, String characterEncoding) {
		setFileType(fileType);
		setCharacterEncoding(characterEncoding);
		setColumnNames(columnNames);
	}

	public List<String> getColumnNames() {
		return Collections.unmodifiableList(columnNames);
	}

	public void setColumnNames(List<String> columnNames) {
		Objects.requireNonNull(columnNames);
		this.columnNames = columnNames;
	}

	public ExportHelperCub.FileType getFileType() {
		return fileType;
	}

	public void setFileType(ExportHelperCub.FileType fileType) {
		Objects.requireNonNull(fileType);
		this.fileType = fileType;
	}

	public String getCharacterEncoding() {
		return characterEncoding;
	}

	public void setCharacterEncoding(String characterEncoding) {
		this.characterEncoding = characterEncoding;
	}

	public void write(OutputStream out, QueryIterator iter) throws Exception {
		String[] columns = getColumnNames().toArray(
				new String[getColumnNames().size()]);
		try (RowExporter exporter = getFileType().createRowExporter(out,
				getCharacterEncoding(), null)) {
			CellGroup group = new CellGroup(null, null, null, columns);
			while (iter.hasNext()) {
				ThreadUtils.checkInterrupt();
				@SuppressWarnings("unchecked")
				T row = (T) iter.next();
				List<String> cellValues = getRowValues(row);
				if (cellValues.size() != columns.length)
					throw new IllegalStateException(
							"the number of incoming cell values didn't match the number of columns ("
									+ cellValues.size() + "!=" + columns.length
									+ ")");
				exporter.writeStrings(group, cellValues);
			}
		}
	}

	protected abstract List<String> getRowValues(T row);
}