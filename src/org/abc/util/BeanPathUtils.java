package org.abc.util;

import java.util.ArrayList;
import java.util.List;

import com.follett.fsc.core.k12.beans.FieldSet;
import com.follett.fsc.core.k12.beans.FieldSetMember;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.beans.path.BeanPath;
import com.follett.fsc.core.k12.beans.path.BeanPathValidationException;
import com.follett.fsc.core.k12.beans.path.BeanTablePath;
import com.follett.fsc.core.k12.business.ModelProperty;
import com.x2dev.utils.StringUtils;

/**
 * This is a collection of static methods that relate to BeanPaths.
 */
public class BeanPathUtils {

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
	@SuppressWarnings({ "rawtypes", "unchecked" })
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

	/**
	 * Return a list of BeanColumnPaths based on a FieldSet.
	 */
	public static List<BeanColumnPath<?, ?, ?>> getBeanPaths(FieldSet fieldSet) {
		List<BeanColumnPath<?, ?, ?>> bcps = new ArrayList<>();
		for (FieldSetMember fsf : fieldSet.getMembers()) {
			String oid = (!StringUtils.isEmpty(fsf.getRelation()) ? fsf
					.getRelation() + "." : "")
					+ fsf.getObjectOid();

			// these sometimes including trailing spaces. Not sure why
			oid = oid.trim();

			String tablePrefix;
			if (oid.startsWith("rel")) {
				tablePrefix = oid.substring(3, 6);
			} else {
				tablePrefix = oid.substring(0, 3);
			}
			Class baseClass = BeanTablePath.getTableByName(tablePrefix)
					.getBeanType();

			BeanColumnPath<?, ?, ?> bcp = (BeanColumnPath<?, ?, ?>) getBeanPath(
					baseClass, oid);
			bcps.add(bcp);
		}
		return bcps;
	}
}
