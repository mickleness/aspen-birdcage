package org.abc.tools.procedures;

import java.util.LinkedHashMap;
import java.util.Map;

import org.abc.tools.Tool;

import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.TempTable;
import com.follett.fsc.core.framework.persistence.adjusters.DistinctAdjuster;
import com.follett.fsc.core.k12.beans.Student;
import com.follett.fsc.core.k12.beans.SystemPreferenceDefinition;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.PreferenceManager;
import com.follett.fsc.core.k12.business.dictionary.DataDictionary;
import com.follett.fsc.core.k12.business.dictionary.DataDictionaryTable;
import com.follett.fsc.core.k12.tools.ToolInput;
import com.follett.fsc.core.k12.tools.procedures.QuickLetterData;
import com.follett.fsc.core.k12.tools.reports.ReportUtils;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.utils.X2BaseException;

/**
 * This extends an existing Quick Letter and emails each message.
 * <p>
 * After importing this bundle: you have to log out and log back in to Aspen to
 * see the new letter.
 * 
 * <h3>Context</h3>
 * <p>
 * Originally we attempted to extend specific letters like
 * QuickLetterAttendanceData. (That is: we created a "Attendance w/ Email"
 * letter.) Unfortunately we couldn't get this idea off the ground. The input
 * parameters for these specialized letters use customized jsp, and it's
 * all-or-nothing with those jsp pages. So as a compromise we created this
 * letter instead.
 */
@Tool(id = "ABC-QL-EMAIL", name = "Quick Letter - Email", input = "QuickLetterEmailInput.xml", category = "Quick Letter", nodes = "key=\"student.std.list\" build-view=\"true\" org1-view=\"true\" health-view=\"true\" iep-view=\"true\" school-view=\"true\" staff-view=\"true\"")
public class QuickLetterEmailData extends QuickLetterData {
	private static final long serialVersionUID = 1L;

	/**
	 * 0 = selection, 1 = snapshot
	 */
	public static final String PARAM_QUERY_BY = "queryBy";

	/**
	 * If we're querying by snapshot, this identifies which snapshot
	 */
	public static final String PARAM_QUERY_STRING = "queryBy";

	QuickLetterEmailHelper emailHelper;

	@Override
	protected String resolveLetterBodyCalculation(String startingHtml,
			X2BaseBean bean) {
		String updatedHtml = super.resolveLetterBodyCalculation(startingHtml,
				bean);
		emailHelper.updateSection(bean, startingHtml, updatedHtml);
		return updatedHtml;
	}

	@Override
	protected String resolveLetterBodyExpression(String startingHtml,
			X2BaseBean bean) {
		String updatedHtml = super.resolveLetterBodyExpression(startingHtml,
				bean);
		emailHelper.updateSection(bean, startingHtml, updatedHtml);
		return updatedHtml;
	}

	@Override
	protected String resolveLetterBodyField(String startingHtml, X2BaseBean bean) {
		String updatedHtml = super.resolveLetterBodyField(startingHtml, bean);
		emailHelper.updateSection(bean, startingHtml, updatedHtml);
		return updatedHtml;
	}

	@Override
	protected void teardown() {
		emailHelper.tearDown();
		super.teardown();

		try {
			emailHelper.sendEmails();
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	/**
	 * This is executed on the application node, then this object is serialized
	 * and the rest of the methods are executed on the report server node.
	 */
	protected void saveState(UserDataContainer userData) throws X2BaseException {
		super.saveState(userData);
		emailHelper = new QuickLetterEmailHelper(userData, getParameters());
	}

	// these methods are required to implement QuickLetterData
	// These are mostly inspired by existing QuickLetterData subclasses:
	// SYS-QL-001, SYS-QL-002 and SYS-QL-003

	@Override
	protected BeanQuery createQuery() {
		ToolInput toolInput = getToolInput();

		int studentsToInclude = Integer.valueOf(
				toolInput.getParameterValue(PARAM_QUERY_BY)).intValue();
		boolean currentSelection = studentsToInclude == 0;
		boolean secondaryStudents = isSchoolContext()
				&& Boolean
						.valueOf(
								PreferenceManager
										.getPreferenceValue(
												getSchool(),
												SystemPreferenceDefinition.SECONDARY_STUDENTS_INCLUDED))
						.booleanValue()
				|| !isSchoolContext()
				&& Boolean
						.valueOf(
								PreferenceManager
										.getPreferenceValue(
												getOrganization(),
												SystemPreferenceDefinition.SECONDARY_STUDENTS_INCLUDED))
						.booleanValue();

		StringBuilder queryString = new StringBuilder(3000);

		/*
		 * Build the outer query that defines the tables and fields involved.
		 * Only include active students from the selected school or district.
		 */
		queryString.append("SELECT DISTINCT S0.* ");
		queryString.append("FROM STUDENT S0 ");

		// JOIN - Current selection
		if (currentSelection) {
			queryString.append("JOIN " + getDatabaseSyntax().getTempTableName()
					+ " ON STD_OID = " + TempTable.DATABASE_STORAGE_OID + " ");
		}

		// JOIN - Primary school
		queryString.append("JOIN SCHOOL K0 ON STD_SKL_OID = K0.SKL_OID ");

		// JOIN - Secondary school
		if (secondaryStudents) {
			queryString
					.append("LEFT OUTER JOIN STUDENT_SCHOOL ON STD_OID = SSK_STD_OID ");

			if (!isSchoolContext()) {
				queryString
						.append("LEFT OUTER JOIN SCHOOL K1 ON SSK_SKL_OID = K1.SKL_OID ");
			}
		}

		queryString.append("WHERE ");

		// Scope for school
		if (isSchoolContext()) {
			queryString.append("(STD_SKL_OID = '");
			queryString.append(getSchool().getOid());
			queryString.append("' ");

			if (secondaryStudents) {
				queryString.append("OR (SSK_SKL_OID = '");
				queryString.append(getSchool().getOid());
				queryString.append("' ");

				queryString.append("AND SSK_CTX_OID = '");
				queryString.append(getCurrentContext().getOid());
				queryString.append("' ");

				queryString.append("AND SSK_ASSOCIATION_TYPE = 1 ");

				queryString.append(") ");
			}

			queryString.append(") ");
		} else {
			int level = getOrganization().getOrganizationDefinition()
					.getLevel();
			queryString.append("(K0.SKL_ORG_OID_");
			queryString.append(level + 1);
			queryString.append(" = '");
			queryString.append(getOrganization().getOid());
			queryString.append("' ");

			if (secondaryStudents) {
				queryString.append("OR (K1.SKL_ORG_OID_");
				queryString.append(level + 1);
				queryString.append(" = '");
				queryString.append(getOrganization().getOid());
				queryString.append("' ");

				queryString.append("AND SSK_CTX_OID = '");
				queryString.append(getCurrentContext().getOid());
				queryString.append("' ");

				queryString.append("AND SSK_ASSOCIATION_TYPE = 1 ");

				queryString.append(") ");
			}

			queryString.append(") ");
		}

		// Scope for student
		switch (studentsToInclude) {
		case 1: // Snapshot
			String subQuery = ReportUtils.getRecordSetSqlSubQuery(
					Student.DICTIONARY_ID,
					(String) getParameter(PARAM_QUERY_STRING), getUser(),
					getSchool(), getOrganization());

			queryString.append(" AND STD_OID IN (" + subQuery + ") ");
			break;

		default:
			// Current selection (join handles students scoping)
			break;
		}

		queryString
				.append("AND STD_OID IN (SELECT DISTINCT STD_OID FROM STUDENT");

		queryString.append(") ");
		queryString.append("ORDER BY STD_NAME_VIEW");

		DataDictionary dictionary = DataDictionary
				.getDistrictDictionary(getBroker().getPersistenceKey());
		DataDictionaryTable table = dictionary
				.findDataDictionaryTableByClass(Student.class.getName());
		DistinctAdjuster adjuster = new DistinctAdjuster(
				table.getPrimaryKeyColumn(), getBroker().getPersistenceKey());

		BeanQuery query = new BeanQuery(Student.class);
		query.setSql(adjuster.adjustSql(queryString.toString()));
		query.setQueryAdjuster(adjuster);
		query.setDistinct(true);

		return query;
	}

	@Override
	protected Class<?> getDataClass() {
		return Student.class;
	}

	@Override
	protected String getCalculationValue(X2BaseBean arg0, String arg1) {
		// No calculations
		return "0";
	}

	@Override
	public Map<String, String> getAvailableCalculations() {
		return new LinkedHashMap<String, String>();
	}
}
