// Catalog Creation Date : 09-19-2017
/*
 * ====================================================================
 *
 * Follett School Solutions
 *
 * Copyright (c) 2002-2016 Follett School Solutions.
 * All rights reserved.
 *
 * ====================================================================
 */

package org.abc.tools.exports;

import java.io.Closeable;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.regex.Pattern;

import com.follett.cust.io.CsvFormat;
import com.follett.cust.io.OidEncoder;
import com.follett.cust.io.exporter.RowExporter;
import com.follett.cust.io.exporter.ZipExporter;
import com.follett.cust.io.exporter.oneroster.BeanId;
import com.follett.cust.io.exporter.oneroster.Field;
import com.follett.cust.io.exporter.oneroster.FieldValidationRuntimeException;
import com.follett.cust.io.exporter.oneroster.FieldValidationRuntimeException.Classification;
import com.follett.cust.io.exporter.oneroster.OneRosterBean;
import com.follett.cust.io.exporter.oneroster.OneRosterBeanType;
import com.follett.cust.io.exporter.oneroster.v1_1.AcademicSession;
import com.follett.cust.io.exporter.oneroster.v1_1.Category;
import com.follett.cust.io.exporter.oneroster.v1_1.ClassType;
import com.follett.cust.io.exporter.oneroster.v1_1.Course;
import com.follett.cust.io.exporter.oneroster.v1_1.Demographics;
import com.follett.cust.io.exporter.oneroster.v1_1.Enrollment;
import com.follett.cust.io.exporter.oneroster.v1_1.Gender;
import com.follett.cust.io.exporter.oneroster.v1_1.GradeLevel;
import com.follett.cust.io.exporter.oneroster.v1_1.LineItem;
import com.follett.cust.io.exporter.oneroster.v1_1.Manifest;
import com.follett.cust.io.exporter.oneroster.v1_1.OrgType;
import com.follett.cust.io.exporter.oneroster.v1_1.Organization;
import com.follett.cust.io.exporter.oneroster.v1_1.Result;
import com.follett.cust.io.exporter.oneroster.v1_1.RoleType;
import com.follett.cust.io.exporter.oneroster.v1_1.ScoreStatus;
import com.follett.cust.io.exporter.oneroster.v1_1.SessionType;
import com.follett.cust.io.exporter.oneroster.v1_1.User;
import com.follett.cust.reports.ExportArbor;
import com.follett.cust.tools.RowResult;
import com.follett.cust.util.MultiKey;
import com.follett.cust.util.MultipleRuntimeException;
import com.follett.fsc.core.framework.persistence.X2Criteria;
import com.follett.fsc.core.k12.beans.Staff;
import com.follett.fsc.core.k12.beans.Student;
import com.follett.fsc.core.k12.beans.SystemPreferenceDefinition;
import com.follett.fsc.core.k12.beans.UserToolDetail;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.beans.path.BeanColumnPath;
import com.follett.fsc.core.k12.business.OrganizationManager;
import com.follett.fsc.core.k12.business.PreferenceManager;
import com.follett.fsc.core.k12.business.StudentManager;
import com.x2dev.sis.model.beans.GradebookColumnDefinition;
import com.x2dev.sis.model.beans.GradebookScore;
import com.x2dev.sis.model.beans.MasterSchedule;
import com.x2dev.sis.model.beans.Schedule;
import com.x2dev.sis.model.beans.ScheduleTerm;
import com.x2dev.sis.model.beans.SisSchool;
import com.x2dev.sis.model.beans.SisStaff;
import com.x2dev.sis.model.beans.SisStudent;
import com.x2dev.sis.model.beans.StudentSchedule;
import com.x2dev.sis.model.beans.path.SisBeanPaths;
import com.x2dev.utils.DataGrid;
import com.x2dev.utils.StringUtils;
import com.x2dev.utils.ThreadUtils;
import com.x2dev.utils.X2BaseException;
import com.x2dev.utils.types.PlainDate;

/**
 * This is modified from Aspen's original
 * com.x2dev.reports.portable.OneRosterExport_1_1 implementation to better
 * support adding non-primary teachers.
 * <p>
 * This produces a zip archive of csv files that comply with the
 * <a href="https://www.imsglobal.org/oneroster-v11-final-csv-tables">One Roster
 * CSV v1.1 specification</a>.
 * <p>
 * The recommended input parameters for this tool are:
 *
 * <pre>
 *     &lt;tool-input allow-school-select="true" district-support="true" allow-year-select="true">
 *         &lt;input name="excludedSchoolOids" data-type="string" display-type="picklist" display-name="Schools to Exclude" required="false">
 *             &lt;picklist field-id="sklSchoolName" multiple="true" required="false" >
 *                 &lt;field id="sklSchoolName" sort="true" />
 *                 &lt;field id="sklSchoolID" />
 *                 &lt;field id="sklInactiveInd" />
 *             &lt;/picklist>
 *         &lt;/input>
 *         &lt;input name="includeContacts" data-type="boolean" display-type="checkbox"
 *             display-name="Include Student Contacts" default-value="true" required="false" />
 *         &lt;input name="includeGrades" data-type="boolean" display-type="checkbox"
 *             display-name="Include Grades/Assignments" default-value="false"
 *             required="false" />
 *         &lt;input name="strictFieldValidation" data-type="boolean"
 *             display-type="checkbox" display-name="Strictly Enforce Required Data"
 *             default-value="true" />
 *         &lt;input name="uidFormat" data-type="integer" display-type="select"
 *             display-name="User ID Format">
 *             &lt;option value="0" display-name="Default" />
 *             &lt;option value="1" display-name="Local ID + Default" />
 *             &lt;option value="2" display-name="State ID + Default" />
 *             &lt;option value="3" display-name="Local ID Only" />
 *             &lt;option value="4" display-name="State ID Only" />
 *         &lt;/input>
 *         &lt;!--  NOTE VALUES ABOVE 10 HAVE NEVER BEEN TESTED, USE IT AT YOUR OWN RISK -->
 *         &lt;input name="maxMillionsOfRecords" data-type="decimal" display-type="hidden"
 *         display-name="Max Millions of Records Limit" default-value="1.0" />
 *     &lt;/tool-input>
 * </pre>
 *
 * @author Follett School Solutions
 */
public class OneRosterExport_1_1_Coteachers extends ExportArbor {

	/**
	 * This exception is thrown when we create too many beans and we want to
	 * abort for fear of eventually triggering a memory error.
	 *
	 * @author Follett Software Company
	 * @copyright 2017
	 */
	public static class RecordLimitException extends Exception {

		public RecordLimitException(String msg) {
			super(msg);
		}
	}

	/**
	 * Given a series of columns for this ScheduleTerm, return the longest one
	 * that is strictly numeric.
	 * <p>
	 * For example, given columns that relate to ["0011", "01", null, "0011,1"]
	 * this will return "0011".
	 *
	 * @param term
	 *            the term to fetch the columns for
	 * @param columnNames
	 *            the names of the columns to fetch and compare.
	 *
	 * @return String the longest numeric column provided, or null if no
	 *         qualifying Strings were identified.
	 */
	private static String getBinaryTermMask(ScheduleTerm term,
			Collection<String> columnNames) {
		List<String> values = new ArrayList<>();
		for (String col : columnNames) {
			values.add((String) term.getFieldValueByBeanPath(col));
		}

		String returnValue = null;
		for (String map : values) {
			if (StringUtils.isNumeric(map)) {
				if (returnValue == null
						|| map.length() > returnValue.length()) {
					returnValue = map;
				}
			}
		}

		return returnValue;
	}

	private static final long serialVersionUID = 1L;

	/**
	 * This parameter maps to a String context (CTX) oid, or if it is empty we
	 * try to export data for all school years.
	 */
	protected static final String PARAM_CONTEXT_OID = "contextOid";

	/**
	 * This parameter maps to a boolean indicating whether contacts should be
	 * included. By default this is assumed to be true.
	 */
	protected static final String PARAM_INCLUDE_CONTACTS = "includeContacts";

	/**
	 * This parameter maps to a boolean indicating whether LineItems and Results
	 * data should be included. By default this is assumed to be true.
	 */
	protected static final String PARAM_INCLUDE_GRADES = "includeGrades";

	/**
	 * This parameter maps to a String school oid, or if it is empty we try to
	 * export data for all schools.
	 */
	protected static final String PARAM_SCHOOL_OID = "schoolOid";
	/**
	 * This parameter maps to a Boolean indicating whether we reject beans that
	 * are missing required data.
	 */
	protected static final String PARAM_STRICT_FIELD_VALIDATION = "strictFieldValidation";

	/**
	 * This key is used in DataGrids to indicate what sheet a record belongs to.
	 */
	protected static final String KEY_ENTRY_NAME = "entry-name";

	/**
	 * This parameter maps to an integer where: 0 = default 1 = prepend Local ID
	 * 2 = prepend State ID 3 = use only Local ID 4 = use only State ID
	 */
	protected static final String PARAM_UID_FORMAT = "uidFormat";

	/**
	 * This parameter was added to allows self-hosted clients to increase the
	 * total amount of millions of records limit
	 */
	protected static final String PARAM_LIMIT = "maxMillionsOfRecords";

	/**
	 * This optional parameter maps to a comma-separated list of school oids to
	 * skip.
	 */
	protected static final String PARAM_EXCLUDED_SCHOOLS = "excludedSchoolOids";

	/**
	 * This helper catalogs how often different errors occurred, and includes a
	 * few sample beans of each error.
	 *
	 * @author Follett Software Company
	 * @copyright 2017
	 */
	class FieldValidationExceptionHandler implements Closeable {

		/**
		 * Details related to a type of exception.
		 *
		 * @author Follett Software Company
		 * @copyright 2017
		 */
		class Details {

			private List<FieldValidationRuntimeException> m_first10exceptions = new ArrayList<>();
			private int uniqueID = m_idCounter++;
			private int m_total;

			/**
			 * Update internal fields to include information from this
			 * exception.
			 *
			 * @param e
			 *            the exception to add to this Details object.
			 */
			public void add(FieldValidationRuntimeException e) {
				m_total++;

				if (m_first10exceptions.size() < 10) {
					m_first10exceptions.add(e);
				}
			}

		}

		private int m_idCounter = 0;
		private UserToolDetail m_detailMessage;
		private int m_prunedBeanCount = 0;
		private List<OneRosterBean> m_prunedBeans = new ArrayList<>();
		/**
		 * The first element of the key is a String message The second element
		 * is the Classification enum The third element is OneRoster bean type
		 * (simple name)
		 */
		private Map<MultiKey, Details> m_validationExceptionMap = new LinkedHashMap<>();

		/**
		 * @param bean
		 */
		public void handlePrunedBean(OneRosterBean bean) {
			m_prunedBeanCount++;
			if (m_prunedBeans.size() < 10) {
				m_prunedBeans.add(bean);
			}
		}

		public void handleRuntimeException(RuntimeException e) {
			List<RuntimeException> allExceptions = extractExceptions(e);
			Iterator<RuntimeException> iter = allExceptions.iterator();
			while (iter.hasNext()) {
				RuntimeException e2 = iter.next();
				if (e2 instanceof FieldValidationRuntimeException) {
					handleFieldValidationRuntimeException(
							(FieldValidationRuntimeException) e2);
					iter.remove();
				}
			}

			if (allExceptions.size() == 1) {
				throw allExceptions.get(0);
			} else if (allExceptions.size() > 0) {
				throw new MultipleRuntimeException(allExceptions);
			}
		}

		/**
		 * This calls {@link #save()}.
		 */
		@Override
		public void close() {
			save();
		}

		/**
		 * This saves this information to a UserToolDetail message.
		 */
		public void save() {
			m_detailMessage = logToolMessage(m_detailMessage, Level.INFO,
					toString());
		}

		/**
		 * This produces a very large block of text summarizing all the
		 * exceptions we've observed.
		 *
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();

			Comparator<Entry<MultiKey, Details>> detailsComparator = new Comparator<Entry<MultiKey, Details>>() {

				@Override
				public int compare(Entry<MultiKey, Details> o1,
						Entry<MultiKey, Details> o2) {
					if (o1 == o2) {
						return 0;
					}

					MultiKey key1 = o1.getKey();
					MultiKey key2 = o2.getKey();

					int i = Integer.compare(
							((Classification) key1.getElement(1)).ordinal(),
							((Classification) key2.getElement(1)).ordinal());

					if (i == 0) {
						int size1 = o1.getValue().m_total;
						int size2 = o2.getValue().m_total;
						i = Integer.compare(size2, size1);
					}

					if (i == 0) {
						return Integer.compare(o1.getValue().uniqueID,
								o2.getValue().uniqueID);
					}
					return i;
				}

			};

			TreeSet<Entry<MultiKey, Details>> sortedSet = new TreeSet<>(
					detailsComparator);
			sortedSet.addAll(m_validationExceptionMap.entrySet());

			for (Entry<MultiKey, Details> entry : sortedSet) {
				Details d = entry.getValue();
				sb.append("The following error affected "
						+ NumberFormat.getInstance().format(d.m_total) + " "
						+ entry.getKey().getElement(2) + " record(s):\n");
				sb.append(entry.getKey().getElement(0) + "\n\n");
				for (FieldValidationRuntimeException ex : d.m_first10exceptions) {
					OneRosterBean bean = ex.getBean();
					String k = bean.getMap().toString();
					Object value = bean.getMap().get(ex.getField().toString());
					if (value instanceof String && ex.getField()
							.getRelationshipType() == User.TYPE) {
						Object format = getParameters().get(PARAM_UID_FORMAT);
						if (Integer.valueOf(3).equals(format)
								|| Integer.valueOf(4).equals(format)) {
							k = (String) value;
						} else {
							String oid = OID_ENCODER.decode((String) value);
							if (oid.startsWith(Staff.OBJECT_PREFIX)) {
								Staff staff = (Staff) getBroker()
										.getBeanByOid(Staff.class, oid);
								k += " (" + staff.getNameView() + ")";
							} else if (oid.startsWith(Student.OBJECT_PREFIX)) {
								Student student = (Student) getBroker()
										.getBeanByOid(Student.class, oid);
								k += " (" + student.getNameView() + ")";
							} else {
								k += " (" + oid + ")";
							}
						}
					} else if (value instanceof String && ex.getField()
							.getRelationshipType() == com.follett.cust.io.exporter.oneroster.v1_1.Class.TYPE) {
						String oid = OID_ENCODER.decode((String) value);
						if (oid.startsWith(MasterSchedule.OBJECT_PREFIX)) {
							MasterSchedule section = (MasterSchedule) getBroker()
									.getBeanByOid(MasterSchedule.class, oid);
							k += " (" + section.getCourseView() + "/"
									+ section.getRoomView() + "/"
									+ section.getSectionNumber() + "/"
									+ section.getStaffView() + "/"
									+ section.getTermView() + ")";
						} else {
							k += " (" + oid + ")";
						}
					} else if (value instanceof String
							&& ex.getField().getRelationshipType() != null) {
						String oid = OID_ENCODER.decode((String) value);
						k += " " + oid + "/"
								+ ex.getField().getRelationshipType();
					}

					sb.append(k + "\n\n");
				}
				int remaining = d.m_total - d.m_first10exceptions.size();
				if (remaining > 0) {
					sb.append("... (and "
							+ NumberFormat.getInstance().format(remaining)
							+ " other record(s))\n");
				}
				sb.append("\n");
			}

			sb.append("\n");
			if (m_prunedBeanCount > 0) {
				sb.append(NumberFormat.getInstance().format(m_prunedBeanCount)
						+ " record(s) were pruned:\n");
				for (OneRosterBean bean : m_prunedBeans) {
					sb.append(bean.getMap() + "\n\n");
				}
				int remaining = m_prunedBeanCount - m_prunedBeans.size();
				if (remaining > 0) {
					sb.append("... (and "
							+ NumberFormat.getInstance().format(remaining)
							+ " other record(s))\n");
				}
			}

			return sb.toString().trim();
		}

		/** Convert a RuntimeException into a list of RuntimeExceptions. */
		private List<RuntimeException> extractExceptions(RuntimeException e) {
			List<RuntimeException> returnValue = new ArrayList<>();
			extractExceptions(returnValue, e);
			return returnValue;
		}

		/**
		 * Recursively populate <code>list</code> with all the detectable
		 * RuntimeExceptions.
		 */
		private void extractExceptions(List<RuntimeException> list,
				RuntimeException e) {
			if (e instanceof MultipleRuntimeException) {
				MultipleRuntimeException mre = (MultipleRuntimeException) e;
				for (RuntimeException e2 : mre.getExceptions()) {
					extractExceptions(list, e2);
				}
			} else {
				list.add(e);
			}
		}

		/** Catalog information about a FieldValidationRuntimeException. */
		private void handleFieldValidationRuntimeException(
				FieldValidationRuntimeException e) {
			MultiKey key = new MultiKey(e.getMessage(), e.getClassification(),
					e.getBean().getClass().getSimpleName());
			Details details = m_validationExceptionMap.get(key);
			if (details == null) {
				details = new Details();
				m_validationExceptionMap.put(key, details);
			}
			details.add(e);
		}
	}

	private static final Pattern CONTINUOUS_ONE_PATTERN = Pattern
			.compile("0*1*0*");

	/**
	 * A Schedule object will generally represent a large block time such as the
	 * 2012-13 school year divided up into ScheduleTerms. But those
	 * ScheduleTerms may actually have a hierarchy resembling:
	 *
	 * <pre>
	 * full year
	 *  semester 1
	 *    term 1
	 *    term 2
	 *  semester 2
	 *    term 3
	 *    term 4
	 * </pre>
	 *
	 * or:
	 *
	 * <pre>
	 * full year
	 *  trimester 1
	 *  trimester 2
	 *  trimester 3
	 * </pre>
	 *
	 * <p>
	 * This method analyzes the term maps (masks) of each ScheduleTerm and
	 * identifies which ScheduleTerm, if any, is its parent.
	 *
	 * @param scheduleTerms
	 *            a collection of ScheduleTerms to analyze.
	 *
	 * @return a map of ScheduleTerms mapped to their parent. If a ScheduleTerm
	 *         has no parent then it is represented as a key in this map.
	 */
	public static Map<ScheduleTerm, ScheduleTerm> createTree(
			Collection<ScheduleTerm> scheduleTerms) {
		Map<ScheduleTerm, ScheduleTerm> returnValue = new HashMap<>();

		for (ScheduleTerm term1 : scheduleTerms) {
			ScheduleTerm bestParent = null;
			double bestParentCtr = Double.MAX_VALUE;
			for (ScheduleTerm term2 : scheduleTerms) {
				if (!term1.equals(term2)) {
					String mask1 = null;
					String mask2 = null;
					for (String col : Arrays.asList(
							ScheduleTerm.COL_GRADE_TERM_MAP,
							ScheduleTerm.COL_UNIVERSAL_TERM_MAP,
							ScheduleTerm.COL_BASE_TERM_MAP)) {
						String v1 = (String) term1.getFieldValueByBeanPath(col);
						String v2 = (String) term2.getFieldValueByBeanPath(col);
						if (v1 != null && v2 != null && (!v1.equals(v2))
								&& v1.length() == v2.length()
								&& StringUtils.isNumeric(v1)
								&& StringUtils.isNumeric(v2)) {
							if (mask1 == null || v1.length() > mask1.length()) {
								mask1 = v1;
								mask2 = v2;
							}
						}
					}

					if (mask1 != null) {
						double parentCtr = 0;
						for (int j = 0; j < mask1.length(); j++) {
							char ch1 = mask1.charAt(j);
							char ch2 = mask2.charAt(j);
							if (ch1 == '0' && ch2 == '1') {
								parentCtr++;
							} else if ((ch1 == '1' && ch2 == '0')
									|| (!CONTINUOUS_ONE_PATTERN.matcher(mask2)
											.matches())) {
								parentCtr = Double.MAX_VALUE;
								break;
							}
						}

						if (parentCtr < bestParentCtr) {
							bestParentCtr = parentCtr;
							bestParent = term2;
						}
					}
				}
			}

			if (bestParent != null) {
				returnValue.put(term1, bestParent);
			}
		}

		return returnValue;
	}

	/**
	 * This wraps Arrays.asList() in an ArrayList<>(), because apparently the
	 * former doesn't support an iterator that can remove elements.
	 *
	 * Also if the argument is a 1-element array where the only element is null:
	 * we return an empty list.
	 */
	public static <K> List<K> asList(K... a) {
		if (a.length == 1 && a[0] == null) {
			return Collections.EMPTY_LIST;
		}
		return new ArrayList<>(Arrays.asList(a));
	}

	private String getSchoolYearId(String trmOid) {
		String uid = createUid(trmOid, null, null);
		BeanId id = new BeanId(AcademicSession.TYPE, uid);
		AcademicSession as = (AcademicSession) m_allBeans.get(id);

		while (as != null) {
			ThreadUtils.checkInterrupt();

			if ("schoolYear".equals(as.getType())) {
				return as.getSourcedId();
			}

			String parentUid = as.getParentSourcedId();
			if (parentUid == null) {
				return null;
			}
			id = new BeanId(AcademicSession.TYPE, parentUid);
			as = (AcademicSession) m_allBeans.get(id);
		}

		return null;
	}

	Map<BeanId, OneRosterBean> m_allBeans = new HashMap<>();
	FieldValidationExceptionHandler m_exceptionHandler;
	Collection<String> unresolvedGradeLevels = new TreeSet<>();

	public static final OidEncoder OID_ENCODER = new OidEncoder(true, true,
			true);

	/**
	 * Create a UID for a bean's oid
	 */
	public String createUid(String beanOid, String localId, String stateId) {
		if (beanOid == null) {
			return null;
		} else if (beanOid.length() == 0) {
			return null;
		}

		String returnValue = OID_ENCODER.encode(beanOid);
		Object format = getParameters().get(PARAM_UID_FORMAT);
		if (Integer.valueOf(1).equals(format)
				&& !StringUtils.isEmpty(localId)) {
			returnValue = localId + "_" + returnValue;
		}
		if (Integer.valueOf(2).equals(format)
				&& !StringUtils.isEmpty(stateId)) {
			returnValue = stateId + "_" + returnValue;
		}
		if (Integer.valueOf(3).equals(format)
				&& !StringUtils.isEmpty(localId)) {
			returnValue = localId;
		}
		if (Integer.valueOf(4).equals(format)
				&& !StringUtils.isEmpty(stateId)) {
			returnValue = stateId;
		}
		return returnValue;
	}

	protected UserToolDetail m_currentStatus;

	public static String convertUidToOid(String oneRosterBeanUid) {
		return OID_ENCODER.decode(oneRosterBeanUid);
	}

	protected Collection<String> m_parentRelationshipCodes = listCodes(
			"Edu Surrogate Parent", "Educate Surragate Pa",
			"Educate Surrogate Pa", "Educational Surrogat", "Father", "Foster",
			"Foster Parent", "Foster Parents", "Mother", "Mother/Father",
			"Parent", "Parent(s)", "Parent/Guardian", "Parents");

	protected Collection<String> m_guardianRelationshipCodes = listCodes(
			"Aunt/Guardian", "DSS/Guardian", "Grandfather/guardian",
			"Grandmother/Guardian", "Grandparents/Guardia", "Guardian",
			"Legal Custodian", "Legal Guardian");

	protected Collection<String> m_ignoredRelationshipCodes = listCodes(
			"DCF Social Worker", "ESP", "Friend", "Neighbor",
			"Ongoing Social Worke", "Self", "Social Worker", "Student",
			"Surrogate");

	/**
	 * The last timestamp when a UserToolDetail message was updated.
	 */
	protected Map<String, Long> m_lastStatusUpdate;

	protected Collection<String> m_relativeRelationshipCodes = listCodes("Aunt",
			"Aunt & Uncle", "Brother", "Cousin", "Grandfather", "Grandmother",
			"Grandparent", "Grandparents", "Maternal Great Aunt", "Relative",
			"Sister", "Step Father", "Step Mother", "Uncle");

	protected boolean m_strictFieldValidation;

	/**
	 * Append an incoming grid to a master grid. Each row has a new key/value
	 * pair added: KEY_ENTRY_NAME -> incomingName.
	 *
	 * @param masterGrid
	 *            the grid to add rows to
	 * @param incomingGrid
	 *            the grid to read rows from
	 * @param incomingName
	 *            the value of KEY_ENTRY_NAME, which ultimately determines which
	 *            csv file each record appears in.
	 */
	protected void append(DataGrid masterGrid, DataGrid incomingGrid,
			String incomingName) {
		for (Map<String, Object> row : incomingGrid.getRows()) {
			row.put(KEY_ENTRY_NAME, incomingName);
			masterGrid.append(row);
		}
	}

	/**
	 * Create the AcademicSession beans, which map directly to Aspen's
	 * ScheduleTerm records.
	 *
	 * @return int the number of academic session records created.
	 */
	@SuppressWarnings("unused")
	protected int createAcademicSessions() throws RecordLimitException {

		Comparator<X2BaseBean> OID_COMPARATOR = new Comparator<X2BaseBean>() {

			@Override
			public int compare(X2BaseBean o1, X2BaseBean o2) {
				return o1.getOid().compareTo(o2.getOid());
			}

		};

		int ctr = 0;

		X2Criteria c = new X2Criteria();

		/*
		 * We observed a case where other beans were being thrown out because
		 * they were missing their term. It turns out the problem was those
		 * beans (despite being related to the given school) somehow pointed to
		 * a grade term that was NOT associated with the same school.
		 *
		 * The simplest solution is: don't add school to the criteria here. Cast
		 * a wider net, and rely on our other pruning mechanisms to purge
		 * unneeded records later.
		 */
		if (false) {
			String schoolOid = (String) getParameter(PARAM_SCHOOL_OID);
			if (!StringUtils.isBlank(schoolOid)) {
				c.addEqualTo(SisBeanPaths.SCHEDULE.school().oid().toString(),
						schoolOid);
			}
		}
		String contextOid = (String) getParameter(PARAM_CONTEXT_OID);
		if (!StringUtils.isBlank(contextOid)) {
			c.addEqualTo(SisBeanPaths.SCHEDULE.districtContextOid().toString(),
					contextOid);
		}

		RowResult.IteratorBuilder<Schedule> builder = new RowResult.IteratorBuilder<>(
				getBroker().getPersistenceKey(), Schedule.class);
		builder.addColumn(SisBeanPaths.SCHEDULE.oid());
		builder.addColumn(SisBeanPaths.SCHEDULE.name());
		builder.addColumn(SisBeanPaths.SCHEDULE.scheduleTerms().oid());
		builder.addColumn(SisBeanPaths.SCHEDULE.scheduleTerms().gradeTermMap());
		builder.addColumn(
				SisBeanPaths.SCHEDULE.scheduleTerms().universalTermMap());
		builder.addColumn(SisBeanPaths.SCHEDULE.scheduleTerms().baseTermMap());
		builder.addColumn(SisBeanPaths.SCHEDULE.scheduleTerms()
				.scheduleTermDates().startDate());
		builder.addColumn(SisBeanPaths.SCHEDULE.scheduleTerms()
				.scheduleTermDates().endDate());
		builder.addColumn(SisBeanPaths.SCHEDULE.districtContext().endDate());
		builder.addOrderBy(SisBeanPaths.SCHEDULE.oid(), true);
		builder.addOrderBy(SisBeanPaths.SCHEDULE.scheduleTerms().oid(), true);

		try (RowResult.Iterator<Schedule> iter = builder
				.createIterator(getBroker(), c)) {
			while (iter.hasNext()) {
				Collection<RowResult<Schedule>> results = iter
						.next(SisBeanPaths.SCHEDULE.oid());
				Collection<ScheduleTerm> terms = new TreeSet<>(OID_COMPARATOR);
				for (RowResult<Schedule> result : results) {
					ScheduleTerm term = (ScheduleTerm) result
							.getBean(SisBeanPaths.SCHEDULE.scheduleTerms());
					if (term != null) {
						terms.add(term);
					}
				}
				String scheduleName = (String) results.iterator().next()
						.getValue(SisBeanPaths.SCHEDULE.name());

				Map<ScheduleTerm, ScheduleTerm> childToParent = createTree(
						terms);

				for (ScheduleTerm term : terms) {
					String mask = getBinaryTermMask(term,
							Arrays.asList(ScheduleTerm.COL_GRADE_TERM_MAP,
									ScheduleTerm.COL_UNIVERSAL_TERM_MAP,
									ScheduleTerm.COL_BASE_TERM_MAP));
					if (mask != null) {
						PlainDate startDate = null;
						PlainDate endDate = null;

						for (RowResult result : results) {
							if (term.getOid().equals(
									result.getValue(SisBeanPaths.SCHEDULE
											.scheduleTerms().oid()))) {
								Date resultStart = (Date) result.getValue(
										SisBeanPaths.SCHEDULE.scheduleTerms()
												.scheduleTermDates()
												.startDate());
								Date resultEnd = (Date) result.getValue(
										SisBeanPaths.SCHEDULE.scheduleTerms()
												.scheduleTermDates().endDate());

								PlainDate resultStartDate = resultStart == null
										? null
										: new PlainDate(resultStart);
								if (resultStartDate != null
										&& (startDate == null || resultStartDate
												.getTime() < startDate
														.getTime())) {
									startDate = resultStartDate;
								}
								PlainDate resultEndDate = resultEnd == null
										? null
										: new PlainDate(resultEnd);
								if (resultEndDate != null && (endDate == null
										|| resultEndDate.getTime() > endDate
												.getTime())) {
									endDate = resultEndDate;
								}
							}
						}

						String oid = term == null ? null : term.getOid();
						String uid = createUid(oid, null, null);
						String name = scheduleName + ": " + term.getName();
						try {
							AcademicSession session = new AcademicSession(uid);
							session.setStartDate(startDate);
							session.setEndDate(endDate);
							session.setTitle(name);

							Date schoolYearEndDate = (Date) results.iterator()
									.next().getValue(SisBeanPaths.SCHEDULE
											.districtContext().endDate());
							GregorianCalendar calendar = new GregorianCalendar();
							calendar.setTime(schoolYearEndDate);
							session.setSchoolYear(Integer
									.toString(calendar.get(Calendar.YEAR)));

							ScheduleTerm parent = childToParent.get(term);
							oid = parent == null ? null : parent.getOid();
							session.setParentSourcedId(
									createUid(oid, null, null));

							int oneCount = mask.replace("0", "").length();
							SessionType sessionType;
							if (oneCount == mask.length()) {
								sessionType = SessionType.SCHOOL_YEAR;
							} else if (oneCount == 2 && mask.length() == 4) {
								sessionType = SessionType.SEMESTER;
							} else {
								sessionType = SessionType.TERM;
							}
							session.setType(sessionType);

							ctr++;
							saveBean(null, false, session);
						} catch (RuntimeException e) {
							m_exceptionHandler.handleRuntimeException(e);
						}
					}
				}

				logStatus(iter.getPosition(), iter.getCount(),
						"academic sessions", !iter.hasNext());
			}
		} catch (RecordLimitException rle) {
			throw rle;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return ctr;
	}

	/**
	 * Create the Enrollment, Class and Course beans, which map directly to
	 * Aspen's StudentSchedule, MasterSchedule, SchoolCourse records.
	 *
	 * @param the
	 *            number of enrollment records saved.
	 */
	protected int createEnrollments() throws RecordLimitException {
		int ctr = 0;

		X2Criteria criteria = new X2Criteria();
		String schoolOid = (String) getParameter(PARAM_SCHOOL_OID);
		if (!StringUtils.isBlank(schoolOid)) {
			criteria.addEqualTo(SisBeanPaths.STUDENT_SCHEDULE.student().school()
					.oid().toString(), schoolOid);
		}
		List<String> excludedSchools = getExludedSchoolOids();
		if (!excludedSchools.isEmpty()) {
			criteria.addNotIn(SisBeanPaths.STUDENT_SCHEDULE.student().school()
					.oid().toString(), excludedSchools);
		}
		String contextOid = (String) getParameter(PARAM_CONTEXT_OID);
		if (!StringUtils.isBlank(contextOid)) {
			criteria.addEqualTo(SisBeanPaths.STUDENT_SCHEDULE.schedule()
					.districtContextOid().toString(), contextOid);
		}

		RowResult.IteratorBuilder<StudentSchedule> builder = new RowResult.IteratorBuilder<>(
				getBroker().getPersistenceKey(), StudentSchedule.class);

		BeanColumnPath sscOid = SisBeanPaths.STUDENT_SCHEDULE.oid();
		BeanColumnPath stdOid = SisBeanPaths.STUDENT_SCHEDULE.student().oid();
		BeanColumnPath stdLocalId = SisBeanPaths.STUDENT_SCHEDULE.student()
				.localId();
		BeanColumnPath stdStateId = SisBeanPaths.STUDENT_SCHEDULE.student()
				.stateId();
		BeanColumnPath sklOid = SisBeanPaths.STUDENT_SCHEDULE.section()
				.schoolCourse().school().oid();
		BeanColumnPath mstOid = SisBeanPaths.STUDENT_SCHEDULE.section().oid();
		BeanColumnPath trmOid = SisBeanPaths.STUDENT_SCHEDULE.section()
				.scheduleTerm().oid();
		BeanColumnPath cskOid = SisBeanPaths.STUDENT_SCHEDULE.section()
				.schoolCourse().oid();
		BeanColumnPath cskCourseDescription = SisBeanPaths.STUDENT_SCHEDULE
				.section().schoolCourse().description();
		BeanColumnPath crsGradeLevel = SisBeanPaths.STUDENT_SCHEDULE.section()
				.schoolCourse().course().gradeLevel();
		BeanColumnPath mstCourseView = SisBeanPaths.STUDENT_SCHEDULE.section()
				.courseView();
		BeanColumnPath mstRoomView = SisBeanPaths.STUDENT_SCHEDULE.section()
				.roomView();
		BeanColumnPath cskCourseNumber = SisBeanPaths.STUDENT_SCHEDULE.section()
				.schoolCourse().number();

		// we populate teacher enrollments two different ways.
		// #1: The MST record includes a "primaryStaff" relationship. This is
		// what we used for several years, until some users on a mailing list
		// pointed out they needed coteachers (nonprimary) teachers too.
		// #2: So we added teachers from the MTC records too.

		// Theoretically the second case is all we ever need. Probably. But that
		// assumes the MTC table is perfectly maintained. Just in case it's not:
		// let's continue to poll both pieces of info. So with this new revision
		// we shouldn't risk *losing* any data, we should only ever add data.

		BeanColumnPath mstStfOid = SisBeanPaths.STUDENT_SCHEDULE.section()
				.primaryStaff().oid();
		BeanColumnPath mstStfLocalId = SisBeanPaths.STUDENT_SCHEDULE.section()
				.primaryStaff().localId();
		BeanColumnPath mstStfStateId = SisBeanPaths.STUDENT_SCHEDULE.section()
				.primaryStaff().stateId();

		BeanColumnPath mtcStfPrimaryIndicator = SisBeanPaths.STUDENT_SCHEDULE
				.section().teacherSections().primaryTeacherIndicator();
		BeanColumnPath mtcStfOid = SisBeanPaths.STUDENT_SCHEDULE.section()
				.teacherSections().staffOid();
		BeanColumnPath mtcStfLocalId = SisBeanPaths.STUDENT_SCHEDULE.section()
				.teacherSections().staff().localId();
		BeanColumnPath mtcStfStateId = SisBeanPaths.STUDENT_SCHEDULE.section()
				.teacherSections().staff().stateId();

		BeanColumnPath stdNameView = SisBeanPaths.STUDENT_SCHEDULE.student()
				.nameView();

		builder.addColumn(sscOid);
		builder.addColumn(stdOid);
		builder.addColumn(stdLocalId);
		builder.addColumn(stdStateId);
		builder.addColumn(sklOid);
		builder.addColumn(mstOid);
		builder.addColumn(trmOid);
		builder.addColumn(cskOid);
		builder.addColumn(cskCourseDescription);
		builder.addColumn(crsGradeLevel);
		builder.addColumn(mstCourseView);
		builder.addColumn(mstRoomView);
		builder.addColumn(cskCourseNumber);
		builder.addColumn(mtcStfPrimaryIndicator);
		builder.addColumn(mtcStfOid);
		builder.addColumn(mtcStfLocalId);
		builder.addColumn(mtcStfStateId);
		builder.addColumn(mstStfOid);
		builder.addColumn(mstStfLocalId);
		builder.addColumn(mstStfStateId);
		builder.addColumn(stdNameView);

		try (RowResult.Iterator<StudentSchedule> iter = builder
				.createIterator(getBroker(), criteria)) {
			while (iter.hasNext()) {
				RowResult<StudentSchedule> row = iter.next();
				String studentUid = createUid((String) row.getValue(stdOid),
						(String) row.getValue(stdLocalId),
						(String) row.getValue(stdStateId));
				String classCode = (String) row.getValue(mstRoomView);
				String nameView = (String) row.getValue(stdNameView);
				try {
					String enrollmentUid = createUid(
							(String) row.getValue(sscOid), null, null);
					String orgUid = createUid((String) row.getValue(sklOid),
							null, null);
					String classUid = createUid((String) row.getValue(mstOid),
							null, null);
					String classTermSourcedId = createUid(
							(String) row.getValue(trmOid), null, null);
					String courseUid = createUid((String) row.getValue(cskOid),
							null, null);
					String classTitle = (String) row
							.getValue(cskCourseDescription);

					String crsGradeLevelValue = (String) row
							.getValue(crsGradeLevel);
					GradeLevel grade = GradeLevel.get(crsGradeLevelValue);

					if (grade == null) {
						unresolvedGradeLevels.add(crsGradeLevelValue);
					}

					String location = (String) row.getValue(mstRoomView);
					String courseCode = (String) row.getValue(cskCourseNumber);

					// TO-DO: class type can "scheduled" or "homeroom": how do
					// we detect homerooms?
					ClassType classType = ClassType.SCHEDULED;

					// TO-DO: these are optional, but someday we could
					// implement:
					List<String> subjects = null;

					String schoolYearId = getSchoolYearId(
							(String) row.getValue(trmOid));

					List<OneRosterBean> beansToSave = new LinkedList<>();

					Enrollment enrollment = new Enrollment(enrollmentUid);
					enrollment.setUserSourcedId(studentUid);
					enrollment.setSchoolSourcedId(orgUid);
					enrollment.setClassSourcedId(classUid);
					enrollment.setPrimary(Boolean.FALSE);
					enrollment.setRole(RoleType.STUDENT);
					beansToSave.add(enrollment);

					com.follett.cust.io.exporter.oneroster.v1_1.Class c = new com.follett.cust.io.exporter.oneroster.v1_1.Class(
							classUid);
					c.setTermSourcedIds(asList(classTermSourcedId));
					c.setCourseSourcedId(courseUid);
					c.setSchoolSourcedId(orgUid);
					c.setTitle(classTitle);
					c.setClassType(classType);
					c.setGrades(asList(grade));
					c.setClassCode(classCode);
					c.setLocation(location);
					c.setSubjects(subjects);
					beansToSave.add(c);

					Course course = new Course(courseUid);
					course.setOrgSourcedId(orgUid);
					course.setSchoolYearId(schoolYearId);
					course.setTitle(classTitle);
					course.setCourseCode(courseCode);
					course.setGrades(asList(grade));
					course.setSubjects(subjects);
					beansToSave.add(course);

					class StaffDescription {
						String staffOid, staffLocalId, staffStateId;
						Boolean isPrimary;
					}

					StaffDescription description1 = new StaffDescription();
					StaffDescription description2 = new StaffDescription();

					description1.staffOid = (String) row.getValue(mtcStfOid);
					description1.staffLocalId = (String) row
							.getValue(mtcStfLocalId);
					description1.staffStateId = (String) row
							.getValue(mtcStfStateId);
					description1.isPrimary = (Boolean) row
							.getValue(mtcStfPrimaryIndicator);

					description2.staffOid = (String) row.getValue(mstStfOid);
					description2.staffLocalId = (String) row
							.getValue(mstStfLocalId);
					description2.staffStateId = (String) row
							.getValue(mstStfStateId);

					// the "mstStf" fields came from a relationship labeled
					// "primaryStaff", so it's safe to assume "isPrimary" should
					// always be true.
					description2.isPrimary = Boolean.TRUE;

					for (StaffDescription description : new StaffDescription[] {
							description1, description2 }) {

						String staffUser = createUid(description.staffOid,
								description.staffLocalId,
								description.staffStateId);

						boolean skipStaffBean = staffUser == null;
						if (!skipStaffBean) {
							String staffEnrollmentUid = createUid(
									(String) row.getValue(mstOid), null, null)
									+ "_" + staffUser;
							Enrollment staffEnrollment = new Enrollment(
									staffEnrollmentUid);
							staffEnrollment.setUserSourcedId(staffUser);
							staffEnrollment.setSchoolSourcedId(orgUid);
							staffEnrollment.setClassSourcedId(classUid);
							staffEnrollment.setPrimary(description.isPrimary);
							staffEnrollment.setRole(RoleType.TEACHER);

							beansToSave.add(staffEnrollment);
						}
					}

					saveBean(row, false,
							beansToSave.toArray(new OneRosterBean[0]));
					ctr++;
				} catch (RuntimeException e) {
					m_exceptionHandler.handleRuntimeException(e);
				}

				logStatus(iter.getPosition(), iter.getCount(), "enrollments",
						!iter.hasNext());
			}
		} catch (RecordLimitException rle) {
			throw rle;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return ctr;
	}

	protected int createLineItems() throws RecordLimitException {
		int ctr = 0;
		String schoolOid = (String) getParameter(PARAM_SCHOOL_OID);

		BeanColumnPath gcdOid = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION.oid();
		BeanColumnPath gcdName = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.columnName();
		BeanColumnPath gcdComment = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.comment();
		BeanColumnPath gcdDateAssigned = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.dateAssigned();
		BeanColumnPath gcdDateDue = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.dateDue();
		BeanColumnPath gcdTotalPoints = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.totalPoints();
		BeanColumnPath gcdMstOid = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.masterSchedule().oid();
		BeanColumnPath gcdMstTrmOid = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.masterSchedule().scheduleTerm().oid();
		BeanColumnPath gcdGctOid = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.columnType().oid();
		BeanColumnPath gcdGctType = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.columnType().columnType();
		BeanColumnPath gcdGctDescription = SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
				.columnType().columnTypeDescription();

		RowResult.IteratorBuilder<GradebookColumnDefinition> builder = new RowResult.IteratorBuilder<>(
				getBroker().getPersistenceKey(),
				GradebookColumnDefinition.class);
		builder.addColumn(gcdOid);
		builder.addColumn(gcdName);
		builder.addColumn(gcdComment);
		builder.addColumn(gcdDateAssigned);
		builder.addColumn(gcdDateDue);
		builder.addColumn(gcdTotalPoints);
		builder.addColumn(gcdMstOid);
		builder.addColumn(gcdMstTrmOid);
		builder.addColumn(gcdGctOid);
		builder.addColumn(gcdGctType);
		builder.addColumn(gcdGctDescription);

		X2Criteria criteria = new X2Criteria();
		if (!StringUtils.isBlank(schoolOid)) {
			criteria.addEqualTo(SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
					.masterSchedule().schedule().school().oid().toString(),
					schoolOid);
		}
		List<String> excludedSchools = getExludedSchoolOids();
		if (!excludedSchools.isEmpty()) {
			criteria.addNotIn(
					SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION.masterSchedule()
							.schedule().school().oid().toString(),
					excludedSchools);
		}
		String contextOid = (String) getParameter(PARAM_CONTEXT_OID);
		if (!StringUtils.isBlank(contextOid)) {
			criteria.addEqualTo(SisBeanPaths.GRADEBOOK_COLUMN_DEFINITION
					.districtContextOid().toString(), contextOid);
		}

		try (RowResult.Iterator<GradebookColumnDefinition> iter = builder
				.createIterator(getBroker(), criteria)) {

			while (iter.hasNext()) {
				RowResult<GradebookColumnDefinition> result = iter.next();

				String gctOid = (String) result.getValue(gcdGctOid);
				String gctType = (String) result.getValue(gcdGctType);
				String gctDesc = (String) result.getValue(gcdGctDescription);
				String categoryUid = createUid(gctOid, null, null);
				String categoryTitle;
				if (StringUtils.isEmpty(gctType)
						&& StringUtils.isEmpty(gctDesc)) {
					categoryTitle = "Untitled";
				} else if (StringUtils.isEmpty(gctType)) {
					categoryTitle = gctDesc;
				} else if (StringUtils.isEmpty(gctDesc)) {
					categoryTitle = gctType;
				} else {
					categoryTitle = gctType + "(" + gctDesc + ")";
				}

				String oid = (String) result.getValue(gcdOid);
				String name = (String) result.getValue(gcdName);
				String comment = (String) result.getValue(gcdComment);
				Date assigned = (Date) result.getValue(gcdDateAssigned);
				Date due = (Date) result.getValue(gcdDateDue);
				Number max = (Number) result.getValue(gcdTotalPoints);
				String classUid = createUid((String) result.getValue(gcdMstOid),
						null, null);
				String newGradingPeriodUid = createUid(
						(String) result.getValue(gcdMstTrmOid), null, null);

				try {
					Category category;
					if (categoryUid == null) {
						category = new Category("GCT-UNCATEGORIZED");
						category.setTitle("Uncategorized");
					} else {
						category = new Category(categoryUid);
						category.setTitle(categoryTitle);
					}

					LineItem lineItem = new LineItem(
							createUid(oid, null, null));
					lineItem.setTitle(name);
					lineItem.setDescription(comment);
					lineItem.setAssignDate(assigned);
					lineItem.setDueDate(due);
					lineItem.setResultValueMin(Float.valueOf(0));
					lineItem.setResultValueMax(max == null ? null
							: Float.valueOf(max.floatValue()));
					lineItem.setClassSourcedId(classUid);
					lineItem.setGradingPeriodSourcedId(newGradingPeriodUid);
					lineItem.setCategorySourcedId(categoryUid);

					saveBean(result, true, lineItem);
					try {
						saveBean(result, false, category);
					} catch (Exception e) {
						// do nothing
					}

					try {
						lineItem.removeInvalidRelationships(m_allBeans);
						lineItem.validateFields();
					} catch (Exception e) {
						m_allBeans.remove(lineItem.getBeanId());
						// the category should be purged during cleanup later
						// IFF
						// no other lineItems refer to it.
					}
					ctr++;
				} catch (RecordLimitException rle) {
					throw rle;
				} catch (RuntimeException e) {
					m_exceptionHandler.handleRuntimeException(e);
				}
				logStatus(iter.getPosition(), iter.getCount(), "assignments",
						!iter.hasNext());
			}
		}

		Iterator<Entry<BeanId, OneRosterBean>> iter = m_allBeans.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Entry<BeanId, OneRosterBean> entry = iter.next();
			OneRosterBean bean = entry.getValue();
			if (bean instanceof Category) {
				try {
					bean.removeInvalidRelationships(m_allBeans);
				} catch (Exception e) {
					iter.remove();
				}
			}
		}

		return ctr;
	}

	/**
	 * Create the Organization beans, which map directly to Aspen's SisSchool
	 * records.
	 *
	 * @return int the number of organization records created.
	 */
	protected int createOrganizations() throws RecordLimitException {
		RowResult.IteratorBuilder<SisSchool> builder = new RowResult.IteratorBuilder<>(
				getBroker().getPersistenceKey(), SisSchool.class);
		builder.addColumn(SisBeanPaths.SCHOOL.oid());
		builder.addColumn(SisBeanPaths.SCHOOL.name());
		builder.addColumn(SisBeanPaths.SCHOOL.schoolId());

		int ctr = 0;
		try (RowResult.Iterator<SisSchool> iter = builder
				.createIterator(getBroker(), new X2Criteria())) {
			while (iter.hasNext()) {
				RowResult<SisSchool> result = iter.next();
				String schoolOid = (String) result
						.getValue(SisBeanPaths.SCHOOL.oid());
				String schoolName = (String) result
						.getValue(SisBeanPaths.SCHOOL.name());
				String schoolId = (String) result
						.getValue(SisBeanPaths.SCHOOL.schoolId());
				String uid = createUid(schoolOid, null, null);

				try {
					Organization organization = new Organization(uid);
					organization.setName(schoolName);
					organization.setType(OrgType.SCHOOL);
					organization.setIdentifier(schoolId);

					ctr++;
					saveBean(result, true, organization);
				} catch (RuntimeException e) {
					m_exceptionHandler.handleRuntimeException(e);
				}

				logStatus(iter.getPosition(), iter.getCount(), "organizations",
						!iter.hasNext());
			}
		} catch (RecordLimitException rle) {
			throw rle;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return ctr;
	}

	protected int createResults() throws RecordLimitException {
		int ctr = 0;

		BeanColumnPath gscOid = SisBeanPaths.GRADEBOOK_SCORE.oid();

		BeanColumnPath gscGcdOid = SisBeanPaths.GRADEBOOK_SCORE
				.columnDefinition().oid();
		BeanColumnPath gscExempt = SisBeanPaths.GRADEBOOK_SCORE
				.exemptIndicator();
		BeanColumnPath gscStdOid = SisBeanPaths.GRADEBOOK_SCORE.student().oid();
		BeanColumnPath gscStdLocalId = SisBeanPaths.GRADEBOOK_SCORE.student()
				.localId();
		BeanColumnPath gscStdStateId = SisBeanPaths.GRADEBOOK_SCORE.student()
				.stateId();
		BeanColumnPath gscScore = SisBeanPaths.GRADEBOOK_SCORE.score();
		BeanColumnPath gscGrmText = SisBeanPaths.GRADEBOOK_SCORE
				.gradebookRemark().text();
		BeanColumnPath gscGrmPrivate = SisBeanPaths.GRADEBOOK_SCORE
				.gradebookRemark().privateIndicator();

		// including the last modified led to memory errors, so let's only use
		// the due date instead.
		// BeanColumnPath gscLastModified =
		// SisBeanPaths.GRADEBOOK_SCORE.lastModifiedTime();

		RowResult.IteratorBuilder<GradebookScore> builder = new RowResult.IteratorBuilder<>(
				getBroker().getPersistenceKey(), GradebookScore.class);
		builder.addColumn(gscOid);
		builder.addColumn(gscGcdOid);
		builder.addColumn(gscExempt);
		builder.addColumn(gscStdOid);
		builder.addColumn(gscStdLocalId);
		builder.addColumn(gscStdStateId);
		builder.addColumn(gscScore);
		builder.addColumn(gscGrmText);
		builder.addColumn(gscGrmPrivate);

		X2Criteria criteria = new X2Criteria();
		String schoolOid = (String) getParameter(PARAM_SCHOOL_OID);
		if (!StringUtils.isBlank(schoolOid)) {
			criteria.addEqualTo(SisBeanPaths.GRADEBOOK_SCORE.columnDefinition()
					.masterSchedule().schedule().school().oid().toString(),
					schoolOid);
		}
		List<String> excludedSchools = getExludedSchoolOids();
		if (!excludedSchools.isEmpty()) {
			criteria.addNotIn(SisBeanPaths.GRADEBOOK_SCORE.columnDefinition()
					.masterSchedule().schedule().school().oid().toString(),
					excludedSchools);
		}
		String contextOid = (String) getParameter(PARAM_CONTEXT_OID);
		if (!StringUtils.isBlank(contextOid)) {
			criteria.addEqualTo(SisBeanPaths.GRADEBOOK_SCORE.columnDefinition()
					.districtContextOid().toString(), contextOid);
		}

		try (RowResult.Iterator<GradebookScore> iter = builder
				.createIterator(getBroker(), criteria)) {
			while (iter.hasNext()) {
				RowResult<GradebookScore> result = iter.next();

				String oid = (String) result.getValue(gscOid);
				String gcdOid = (String) result.getValue(gscGcdOid);
				Boolean exempt = (Boolean) result.getValue(gscExempt);
				String stdOid = (String) result.getValue(gscStdOid);
				String stdLocalId = (String) result.getValue(gscStdLocalId);
				String stdStateId = (String) result.getValue(gscStdStateId);
				String score = (String) result.getValue(gscScore);
				String remarkText = (String) result.getValue(gscGrmText);
				Boolean privateRemark = (Boolean) result
						.getValue(gscGrmPrivate);

				String userUid = createUid(stdOid, stdLocalId, stdStateId);
				String lineItemUid = createUid(gcdOid, null, null);
				String uid = createUid(oid, null, null);

				BeanId lineItemId = new BeanId(LineItem.TYPE, lineItemUid);
				BeanId userId = new BeanId(User.TYPE, userUid);
				try {
					Result r = new Result(uid);

					if (privateRemark == null
							|| privateRemark.booleanValue() == false) {
						r.setComment(remarkText);
					}
					r.setLineItemSourcedId(lineItemUid);
					Float scoreNumber;
					try {
						scoreNumber = Float.valueOf(score);
					} catch (NumberFormatException e) {
						throw new FieldValidationRuntimeException(r,
								Result.FIELD_SCORE,
								FieldValidationRuntimeException.Classification.INVALID_FIELD,
								"The score \"" + score + "\" must a number.");
					}
					ScoreStatus scoreStatus = ScoreStatus.FULLY_GRADED;
					if (exempt != null && exempt.booleanValue()) {
						scoreStatus = ScoreStatus.EXEMPT;
					}
					LineItem lineItem = (LineItem) m_allBeans.get(lineItemId);

					Date scoreDate = lineItem == null ? null
							: lineItem.getDueDate();
					r.setScore(scoreNumber);
					r.setScoreDate(scoreDate);
					r.setScoreStatus(scoreStatus);
					r.setStudentSourcedId(userUid);

					if (!m_allBeans.containsKey(lineItemId)) {
						throw new FieldValidationRuntimeException(r,
								Result.FIELD_LINE_ITEM_SOURCED_ID,
								FieldValidationRuntimeException.Classification.INVALID_RELATIONSHIP,
								"This result doesn't relate to a valid assignment.");
					} else if (!m_allBeans.containsKey(userId)) {
						throw new FieldValidationRuntimeException(r,
								Result.FIELD_STUDENT_SOURCED_ID,
								FieldValidationRuntimeException.Classification.INVALID_RELATIONSHIP,
								"This result doesn't relate to a valid user.");
					}

					r.removeInvalidRelationships(m_allBeans);

					saveBean(result, false, r);
					ctr++;
				} catch (RecordLimitException rle) {
					throw rle;
				} catch (RuntimeException e) {
					m_exceptionHandler.handleRuntimeException(e);
				}
			}
			logStatus(iter.getPosition(), iter.getCount(), "scores",
					!iter.hasNext());
		}
		return ctr;
	}

	/**
	 * This makes sure every category (including demographics) is given a
	 * separate tab, because the specification says that file must be present
	 * (even if it is empty).
	 *
	 * @see com.follett.cust.reports.ExportArbor#createTabs(com.x2dev.utils.DataGrid)
	 */
	@Override
	protected Map<Object, DataGrid> createTabs(DataGrid dataGrid) {
		Map<Object, DataGrid> returnValue = super.createTabs(dataGrid);

		DataGrid manifestGrid = new DataGrid();
		manifestGrid.append(Manifest
				.createManifest((Collection) returnValue.keySet(), "1.1a"));
		returnValue.put("manifest", manifestGrid);

		return returnValue;
	}

	/**
	 * Create the User beans, which map to either Aspen's SisStudent,
	 * StudentContact, or SisStaff records.
	 *
	 * @return int the number of user records created.
	 */
	protected int createUsers() throws RecordLimitException {
		int ctr = 0;
		int iterCtr = 0;

		// step 1: students and their contacts ("agents")
		{
			boolean includeContacts = !Boolean.FALSE
					.equals(getParameter(PARAM_INCLUDE_CONTACTS));

			X2Criteria criteria = new X2Criteria();
			String schoolOid = (String) getParameter(PARAM_SCHOOL_OID);
			if (!StringUtils.isBlank(schoolOid)) {
				criteria.addEqualTo(
						SisBeanPaths.STUDENT.school().oid().toString(),
						schoolOid);
			}
			List<String> excludedSchools = getExludedSchoolOids();
			if (!excludedSchools.isEmpty()) {
				criteria.addNotIn(
						SisBeanPaths.STUDENT.school().oid().toString(),
						excludedSchools);
			}
			criteria.addAndCriteria(
					StudentManager.getActiveStudentStatusCriteria(
							OrganizationManager
									.getRootOrganization(getBroker()),
							SisStudent.COL_ENROLLMENT_STATUS));

			BeanColumnPath stdOid = SisBeanPaths.STUDENT.oid();
			BeanColumnPath stdNameView = SisBeanPaths.STUDENT.nameView();
			BeanColumnPath stdPsnEmail = SisBeanPaths.STUDENT.person()
					.email01();
			BeanColumnPath stdPsnLastName = SisBeanPaths.STUDENT.person()
					.lastName();
			BeanColumnPath stdPsnMiddleName = SisBeanPaths.STUDENT.person()
					.middleName();
			BeanColumnPath stdPsnFirstName = SisBeanPaths.STUDENT.person()
					.firstName();
			BeanColumnPath stdPsnPhone = SisBeanPaths.STUDENT.person()
					.phone01();
			BeanColumnPath stdPsnUsrLogin = SisBeanPaths.STUDENT.person().user()
					.loginName();
			BeanColumnPath stdLocalId = SisBeanPaths.STUDENT.localId();
			BeanColumnPath stdStateId = SisBeanPaths.STUDENT.stateId();
			BeanColumnPath stdSklOid = SisBeanPaths.STUDENT.schoolOid();
			BeanColumnPath stdEnrStatus = SisBeanPaths.STUDENT
					.enrollmentStatus();
			BeanColumnPath stdPsnDob = SisBeanPaths.STUDENT.person().dob();
			BeanColumnPath stdPsnGenderCode = SisBeanPaths.STUDENT.person()
					.genderCode();
			BeanColumnPath stdGradeLevel = SisBeanPaths.STUDENT.gradeLevel();

			BeanColumnPath ctjOid = SisBeanPaths.STUDENT.contacts().oid();
			BeanColumnPath ctjRelationship = SisBeanPaths.STUDENT.contacts()
					.relationshipCode();
			BeanColumnPath ctjPsnEmail = SisBeanPaths.STUDENT.contacts()
					.contact().person().email01();
			BeanColumnPath ctjPsnLastName = SisBeanPaths.STUDENT.contacts()
					.contact().person().lastName();
			BeanColumnPath ctjPsnMiddleName = SisBeanPaths.STUDENT.contacts()
					.contact().person().middleName();
			BeanColumnPath ctjPsnFirstName = SisBeanPaths.STUDENT.contacts()
					.contact().person().firstName();
			BeanColumnPath ctjPsnPhone = SisBeanPaths.STUDENT.contacts()
					.contact().person().phone01();
			BeanColumnPath ctjPsnDob = SisBeanPaths.STUDENT.contacts().contact()
					.person().dob();
			BeanColumnPath ctjPsnGenderCode = SisBeanPaths.STUDENT.contacts()
					.contact().person().genderCode();
			BeanColumnPath ctjPsnUsrLogin = SisBeanPaths.STUDENT.contacts()
					.contact().person().user().loginName();

			RowResult.IteratorBuilder<SisStudent> builder = new RowResult.IteratorBuilder<>(
					getBroker().getPersistenceKey(), SisStudent.class);
			builder.addColumn(stdOid);
			builder.addColumn(stdGradeLevel);
			builder.addColumn(stdNameView);
			builder.addColumn(stdPsnEmail);
			builder.addColumn(stdPsnLastName);
			builder.addColumn(stdPsnMiddleName);
			builder.addColumn(stdPsnFirstName);
			builder.addColumn(stdPsnPhone);
			builder.addColumn(stdPsnUsrLogin);
			builder.addColumn(stdPsnDob);
			builder.addColumn(stdPsnGenderCode);
			builder.addColumn(stdLocalId);
			builder.addColumn(stdStateId);
			builder.addColumn(stdSklOid);
			builder.addColumn(stdEnrStatus);

			if (includeContacts) {
				builder.addColumn(ctjOid);
				builder.addColumn(ctjRelationship);
				builder.addColumn(ctjPsnEmail);
				builder.addColumn(ctjPsnLastName);
				builder.addColumn(ctjPsnFirstName);
				builder.addColumn(ctjPsnPhone);
				builder.addColumn(ctjPsnUsrLogin);
			}
			builder.addOrderBy(stdOid, true);

			try (RowResult.Iterator<SisStudent> iter = builder
					.createIterator(getBroker(), criteria)) {
				while (iter.hasNext()) {
					Collection<RowResult<SisStudent>> results = iter
							.next(stdOid);
					RowResult<SisStudent> result = results.iterator().next();
					String nameView = (String) result
							.getValue(SisBeanPaths.STUDENT.nameView());

					try {
						String studentOid = (String) result.getValue(stdOid);
						String email = (String) result.getValue(stdPsnEmail);
						String familyName = (String) result
								.getValue(stdPsnLastName);
						String middleName = (String) result
								.getValue(stdPsnMiddleName);
						String givenName = (String) result
								.getValue(stdPsnFirstName);
						String identifier = (String) result
								.getValue(stdLocalId);
						String phone = (String) result.getValue(stdPsnPhone);
						String username = (String) result
								.getValue(stdPsnUsrLogin);
						String stdSchoolOid = (String) result
								.getValue(stdSklOid);
						String stdGradeLevelValue = (String) result
								.getValue(stdGradeLevel);
						GradeLevel grade = GradeLevel.get(stdGradeLevelValue);

						if (grade == null) {
							unresolvedGradeLevels.add(stdGradeLevelValue);
						}

						Boolean enabledUser = Boolean
								.valueOf(StudentManager.isActiveStudent(
										getOrganization(), (String) result
												.getValue(stdEnrStatus)));

						String uid = createUid(studentOid,
								(String) result.getValue(stdLocalId),
								(String) result.getValue(stdStateId));
						User user = new User(uid);

						if (grade != null) {
							user.setGrades(asList(grade));
						}
						user.setEnabledUser(enabledUser);
						user.setEmail(email);
						user.setFamilyName(familyName);
						user.setMiddleName(middleName);
						user.setGivenName(givenName);
						user.setIdentifier(identifier);
						user.setPhone(phone);
						user.setRole(RoleType.STUDENT);
						user.setUsername(username);

						String schoolId = createUid(stdSchoolOid, null, null);
						addOrg(user, schoolId);

						ctr++;
						saveBean(result, true, user);
						saveUserDemographic(user,
								(Date) result.getValue(stdPsnDob),
								(String) result.getValue(stdPsnGenderCode));

						for (RowResult<SisStudent> contact : results) {
							try {
								String contactOid = (String) contact
										.getValue(ctjOid);
								if (contactOid != null) {

									email = (String) contact
											.getValue(ctjPsnEmail);
									familyName = (String) contact
											.getValue(ctjPsnLastName);
									middleName = (String) contact
											.getValue(ctjPsnMiddleName);
									givenName = (String) contact
											.getValue(ctjPsnFirstName);
									identifier = (String) contact
											.getValue(ctjRelationship);
									phone = (String) contact
											.getValue(ctjPsnPhone);
									username = (String) contact
											.getValue(ctjPsnUsrLogin);

									String contactUid = createUid(contactOid,
											null, null);
									User contactUser = new User(contactUid);

									contactUser.setEnabledUser(enabledUser);
									contactUser.setEmail(email);
									contactUser.setFamilyName(familyName);
									contactUser.setGivenName(givenName);
									contactUser.setIdentifier(identifier);
									contactUser.setPhone(phone);
									RoleType contactRole = getRole(contactUser,
											User.FIELD_ROLE, identifier);
									contactUser.setRole(contactRole);
									contactUser.setUsername(username);

									addOrg(contactUser, schoolId);

									if (contactRole == null) {
										throw new FieldValidationRuntimeException(
												contactUser, User.FIELD_ROLE,
												FieldValidationRuntimeException.Classification.INVALID_FIELD,
												"Unrecognized relationship code \""
														+ identifier
														+ "\". Supported values are: "
														+ Arrays.asList(RoleType
																.values()));
									}

									ctr++;
									saveBean(contact, false, contactUser);

									// the student should refer to this contact
									addAgent(user, contactUid);
									// the contact should refer to the student
									addAgent(contactUser, uid);

									saveUserDemographic(contactUser,
											(Date) result.getValue(ctjPsnDob),
											(String) result.getValue(
													ctjPsnGenderCode));
								}
							} catch (RuntimeException e) {
								m_exceptionHandler.handleRuntimeException(e);
							}
						}
					} catch (RuntimeException e) {
						m_exceptionHandler.handleRuntimeException(e);
					}

					logStatus(iter.getPosition(), iter.getCount(), "students",
							!iter.hasNext());
				}
			} catch (RecordLimitException rle) {
				throw rle;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		// step 2: staff
		{
			String activeStatus = PreferenceManager.getPreferenceValue(
					getOrganization(),
					SystemPreferenceDefinition.STAFF_ACTIVE_CODE);
			X2Criteria criteria = new X2Criteria();
			String schoolOid = (String) getParameter(PARAM_SCHOOL_OID);
			if (!StringUtils.isBlank(schoolOid)) {
				criteria.addEqualTo(
						SisBeanPaths.STAFF.school().oid().toString(),
						schoolOid);
			}
			List<String> excludedSchools = getExludedSchoolOids();
			if (!excludedSchools.isEmpty()) {
				criteria.addNotIn(SisBeanPaths.STAFF.school().oid().toString(),
						excludedSchools);
			}
			criteria.addEqualTo(SisBeanPaths.STAFF.status().toString(),
					activeStatus);

			RowResult.IteratorBuilder<SisStaff> builder = new RowResult.IteratorBuilder<>(
					getBroker().getPersistenceKey(), SisStaff.class);
			builder.addColumn(SisBeanPaths.STAFF.oid());
			builder.addColumn(SisBeanPaths.STAFF.status());
			builder.addColumn(SisBeanPaths.STAFF.nameView());
			builder.addColumn(SisBeanPaths.STAFF.person().email01());
			builder.addColumn(SisBeanPaths.STAFF.person().lastName());
			builder.addColumn(SisBeanPaths.STAFF.person().middleName());
			builder.addColumn(SisBeanPaths.STAFF.person().firstName());
			builder.addColumn(SisBeanPaths.STAFF.person().phone01());
			builder.addColumn(SisBeanPaths.STAFF.person().dob());
			builder.addColumn(SisBeanPaths.STAFF.person().genderCode());
			builder.addColumn(SisBeanPaths.STAFF.person().user().loginName());
			builder.addColumn(SisBeanPaths.STAFF.localId());
			builder.addColumn(SisBeanPaths.STAFF.stateId());
			builder.addColumn(SisBeanPaths.STAFF.schoolOid());
			builder.addColumn(SisBeanPaths.STAFF.staffType());

			try (RowResult.Iterator<SisStaff> iter = builder
					.createIterator(getBroker(), criteria)) {
				while (iter.hasNext()) {
					RowResult<SisStaff> result = iter.next();
					String nameView = (String) result
							.getValue(SisBeanPaths.STAFF.nameView());

					try {
						String staffOid = (String) result
								.getValue(SisBeanPaths.STAFF.oid());
						String uid = createUid(staffOid,
								(String) result
										.getValue(SisBeanPaths.STAFF.localId()),
								(String) result.getValue(
										SisBeanPaths.STAFF.stateId()));
						User user = new User(uid);

						String email = (String) result.getValue(
								SisBeanPaths.STAFF.person().email01());
						String familyName = (String) result.getValue(
								SisBeanPaths.STAFF.person().lastName());
						String middleName = (String) result.getValue(
								SisBeanPaths.STAFF.person().middleName());
						String givenName = (String) result.getValue(
								SisBeanPaths.STAFF.person().firstName());
						String identifier = (String) result
								.getValue(SisBeanPaths.STAFF.localId());
						String phone = (String) result.getValue(
								SisBeanPaths.STAFF.person().phone01());
						String username = (String) result.getValue(
								SisBeanPaths.STAFF.person().user().loginName());
						String stfSchoolOid = (String) result
								.getValue(SisBeanPaths.STAFF.schoolOid());
						RoleType role = RoleType.get((String) result
								.getValue(SisBeanPaths.STAFF.staffType()));
						Boolean enabledUser = Boolean
								.valueOf(activeStatus.equals(result.getValue(
										SisBeanPaths.STAFF.status())));

						user.setEnabledUser(enabledUser);
						user.setEmail(email);
						user.setFamilyName(familyName);
						user.setMiddleName(middleName);
						user.setGivenName(givenName);
						user.setIdentifier(identifier);
						user.setPhone(phone);

						if (role == null) {
							// assume if we haven't flagged a staff as anything
							// else that we'll call
							// them a teacher. Teachers who aren't collected to
							// any enrollment
							// records are dropped at the end of this export
							role = RoleType.TEACHER;
						}

						user.setRole(role);
						user.setUsername(username);

						String schoolId = createUid(stfSchoolOid, null, null);
						addOrg(user, schoolId);

						ctr++;
						saveBean(result, true, user);

						saveUserDemographic(user,
								(Date) result.getValue(
										SisBeanPaths.STAFF.person().dob()),
								(String) result.getValue(SisBeanPaths.STAFF
										.person().genderCode()));

					} catch (RuntimeException e) {
						m_exceptionHandler.handleRuntimeException(e);
					}
					logStatus(iter.getPosition(), iter.getCount(), "staff",
							!iter.hasNext());
				}
			} catch (RecordLimitException rle) {
				throw rle;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		return ctr;
	}

	@Override
	protected RowExporter[] getRowExporters(OutputStream out,
			boolean includeAdjacentFile) throws Exception {
		CsvFormat csvFormat = new CsvFormat(Charset.forName("UTF-8"), ',',
				Character.valueOf('"'), true);
		RowExporter exporter = new ZipExporter.Csv(csvFormat, out);
		return new RowExporter[] { exporter };
	}

	@Override
	protected DataGrid gatherData() throws Exception {
		try {
			DataGrid masterGrid = new DataGrid();

			boolean includeGrades = !Boolean.FALSE
					.equals(getParameter(PARAM_INCLUDE_GRADES));

			// First we create several beans, then we validate and prune them.

			int orgCount = createOrganizations();
			logToolMessage(Level.INFO,
					"Logged " + NumberFormat.getInstance().format(orgCount)
							+ " Organizations.",
					false);
			int academicSessionCount = createAcademicSessions();
			logToolMessage(Level.INFO,
					"Logged "
							+ NumberFormat.getInstance()
									.format(academicSessionCount)
							+ " Academic Sessions.",
					false);
			int userCount = createUsers();
			logToolMessage(Level.INFO, "Logged "
					+ NumberFormat.getInstance().format(userCount) + " Users.",
					false);
			int enrollmentCount = createEnrollments();
			logToolMessage(Level.INFO,
					"Logged "
							+ NumberFormat.getInstance().format(enrollmentCount)
							+ " Enrollments.",
					false);

			if (includeGrades) {
				int lineItemCount = createLineItems();
				logToolMessage(Level.INFO, "Logged "
						+ NumberFormat.getInstance().format(lineItemCount)
						+ " LineItems.", false);

				int resultCount = createResults();
				logToolMessage(Level.INFO,
						"Logged "
								+ NumberFormat.getInstance().format(resultCount)
								+ " Results.",
						false);
			}

			if (true || !unresolvedGradeLevels.isEmpty()) {
				logToolMessage(Level.INFO,
						"The following grade levels are unresolved: "
								+ unresolvedGradeLevels
								+ "\n\nThis means these values were identified in COURSE and STUDENT records in Aspen, but this export was unable to convert those to One Roster's specifications.",
						false);
			}

			boolean repeat;
			do {
				repeat = false;
				int ctr = 0;
				int iterCtr = 0;
				int max = m_allBeans.values().size();
				Iterator<OneRosterBean> iter = m_allBeans.values().iterator();
				while (iter.hasNext()) {
					iterCtr++;

					OneRosterBean bean = iter.next();
					OneRosterBean copy = bean.clone();
					Collection<Field> removedFields = null;
					try {
						removedFields = bean
								.removeInvalidRelationships(m_allBeans);
						if (!removedFields.isEmpty()) {
							repeat = true;
							ctr++;
						}
						if (m_strictFieldValidation) {
							bean.validateFields();
						}
					} catch (RuntimeException e) {
						List<Field> removedRequiredFields = new ArrayList<>();
						if (removedFields != null && !removedFields.isEmpty()) {
							for (Field field : removedFields) {
								if (field.isRequired()) {
									removedRequiredFields.add(field);
								}
							}
						}

						if (!removedRequiredFields.isEmpty()) {
							FieldValidationRuntimeException e2 = new FieldValidationRuntimeException(
									copy,
									removedRequiredFields.iterator().next(),
									FieldValidationRuntimeException.Classification.INVALID_RELATIONSHIP,
									"The following required field(s) were unresolved so they were removed: "
											+ removedRequiredFields);
							m_exceptionHandler.handleRuntimeException(e2);
						} else {
							m_exceptionHandler.handleRuntimeException(e);
						}
						iter.remove();
					}
					logStatus(iterCtr, max, "validations", !iter.hasNext());
				}

				if (!repeat) {
					repeat = pruneOrphanedNodes();
				}
			} while (repeat);

			// convert m_allBeans into a DataGrid for export:

			for (OneRosterBean bean : m_allBeans.values()) {
				Map<String, Object> map = bean.getMap();
				map.put(KEY_ENTRY_NAME, bean.getBeanType().getSimpleName());
				for (String key : map.keySet()
						.toArray(new String[map.size()])) {
					Object value = map.get(key);
					if (value instanceof Boolean) {
						value = value.toString().toLowerCase();
					}
					if (value instanceof Collection) {
						try {
							value = toString((Collection) value);
							map.put(key, value);
						} catch (RuntimeException e) {
							logToolMessage(Level.WARNING,
									"key: " + key + " value: " + value, false);
							throw e;
						}
					}
				}
				masterGrid.append(map);
			}
			m_currentStatus = logToolMessage(m_currentStatus, Level.INFO,
					"Built grid with " + NumberFormat.getInstance()
							.format(masterGrid.rowCount()) + " rows.");

			return masterGrid;
		} finally {
			m_exceptionHandler.close();
		}
	}

	@Override
	protected String[] getColumnsForTab(DataGrid masterGrid, Object tabKey,
			DataGrid tabData) {
		if ("manifest".equals(tabKey)) {
			return new String[] { "propertyName", "value" };
		}

		Map<String, Field> fields = null;
		for (OneRosterBeanType type : Manifest.getBeanTypes()) {
			if (type.getSimpleName().equals(tabKey)) {
				fields = type.getStandardFields();
			}
		}

		if (fields == null) {
			throw new IllegalArgumentException(
					"Unrecognized tab key \"" + tabKey + "\".");
		}

		List<String> fieldNames = new ArrayList<>(fields.keySet());
		return fieldNames.toArray(new String[fieldNames.size()]);
	}

	/**
	 * Returns CSV_ZIP, per One Roster requirements.
	 *
	 * @see com.follett.cust.reports.ExportArbor#getFileType()
	 */
	@Override
	protected FileType getFileType() {
		return FileType.CSV_ZIP;
	}

	protected List<String> getExludedSchoolOids() {
		// this is wordier than it needs to be, but I forget how exactly params
		// are encoded:
		String str = (String) getParameter(PARAM_EXCLUDED_SCHOOLS);
		if (str != null)
			str = str.trim();
		if (str == null || str.isEmpty())
			return Collections.EMPTY_LIST;
		List<String> returnValue = new LinkedList<>();
		for (String k : str.split(",")) {
			k = k.trim();
			if (!k.isEmpty())
				returnValue.add(k);
		}
		return returnValue;
	}

	/**
	 * Given an Aspen relationship code, this identifies the appropriate One
	 * Roster role value.
	 */
	protected RoleType getRole(OneRosterBean bean, Field field,
			String relationshipCode) {
		String s = strip(relationshipCode);
		if (m_parentRelationshipCodes.contains(s)) {
			return RoleType.PARENT;
		}
		if (m_guardianRelationshipCodes.contains(s)) {
			return RoleType.GUARDIAN;
		}
		if (m_relativeRelationshipCodes.contains(s)) {
			return RoleType.RELATIVE;
		}
		if (s == null || m_ignoredRelationshipCodes.contains(s)) {
			// this will eventually lead to a ValidationException, but we don't
			// need to rush that
			// here

			return null;
		}
		throw new FieldValidationRuntimeException(bean, field,
				FieldValidationRuntimeException.Classification.INVALID_FIELD,
				"Unrecognized relationship code \"" + relationshipCode
						+ "\". Supported values are: "
						+ Arrays.asList(RoleType.values()));
	}

	@Override
	protected void initialize() throws X2BaseException {
		m_breakCriteria = KEY_ENTRY_NAME;
		m_strictFieldValidation = Boolean.TRUE
				.equals(getParameter(PARAM_STRICT_FIELD_VALIDATION));

		logToolMessage(Level.INFO, "strict = " + m_strictFieldValidation,
				false);
		super.initialize();

		logToolMessage(Level.INFO, getParameters().toString(), false);
		m_exceptionHandler = new FieldValidationExceptionHandler();
	}

	/**
	 * Return true if a couple of seconds have passed and we should offer a
	 * status update.
	 *
	 * @param id
	 *            an ID indicating which UserToolDetail message should be
	 *            updated.
	 * @return true if a couple of seconds have passed and we should offer a
	 *         status update.
	 */
	protected boolean isStatusUpdate(String id) {
		long time = System.currentTimeMillis();
		if (m_lastStatusUpdate == null) {
			m_lastStatusUpdate = new HashMap<>();
		}
		Long lastStatusUpdate = m_lastStatusUpdate.get(id);
		if (lastStatusUpdate == null) {
			lastStatusUpdate = Long.valueOf(-1);
		}
		long elapsed = time - lastStatusUpdate.longValue();
		boolean returnValue = elapsed > 2500;
		if (returnValue) {
			m_lastStatusUpdate.put(id, Long.valueOf(time));
		}
		return returnValue;
	}

	/**
	 * Update the m_currentStatus message so we can see what step we're on and
	 * how progress is proceeding.
	 *
	 * @param ctr
	 *            the index of the element being processed.
	 * @param max
	 *            the maximum number of elements we expect to process.
	 * @param label
	 *            the element being processed. This is the [Z] in the
	 *            expression: "Processed X of Y [Z]..."
	 * @param forceUpdateImmediately
	 *            if true then we will immediately log this message. If false
	 *            then we only update this message when
	 *            {@link #isStatusUpdate(String)} returns true
	 */
	protected void logStatus(int ctr, int max, String label,
			boolean forceUpdateImmediately) {
		ThreadUtils.checkInterrupt();
		if (forceUpdateImmediately || isStatusUpdate("generic-status")) {
			m_currentStatus = logToolMessage(m_currentStatus, Level.INFO,
					"Processed " + NumberFormat.getInstance().format(ctr)
							+ " of " + NumberFormat.getInstance().format(max)
							+ " " + label + "...");
		}
	}

	/**
	 * Starting with enrollment and user beans: track every possible record of
	 * interest in this system, and prune/purge orphaned beans.
	 *
	 * For example: if at one point we added a bean for a school year
	 * (AcademicSession), but none of our enrollment records refer to it: then
	 * we purge it.
	 *
	 * @return boolean true if node were pruned, false if no changes were
	 *         necessary.
	 */
	protected boolean pruneOrphanedNodes() {
		Collection<BeanId> requiredNodes = new HashSet<>();
		for (OneRosterBean bean : m_allBeans.values()) {
			if (bean instanceof Enrollment) {
				Enrollment e = (Enrollment) bean;
				requiredNodes.add(bean.getBeanId());
				requiredNodes.add(new BeanId(
						com.follett.cust.io.exporter.oneroster.v1_1.Class.TYPE,
						e.getClassSourcedId()));
				requiredNodes.add(
						new BeanId(Organization.TYPE, e.getSchoolSourcedId()));
				requiredNodes.add(new BeanId(User.TYPE, e.getUserSourcedId()));
			} else if (bean instanceof User) {
				requiredNodes.add(bean.getBeanId());
			}
		}

		boolean repeat;
		do {
			repeat = false;
			for (OneRosterBean bean : m_allBeans.values()) {
				ThreadUtils.checkInterrupt();
				if (bean instanceof User
						&& requiredNodes.contains(bean.getBeanId())) {
					User u = (User) bean;
					List<String> agents = u.getAgents();
					if (agents != null) {
						for (String agentId : agents) {
							if (requiredNodes
									.add(new BeanId(User.TYPE, agentId))) {
								repeat = true;
							}
						}
					}
					List<String> orgs = u.getOrgSourcedIds();
					if (orgs != null) {
						for (String org : orgs) {
							requiredNodes
									.add(new BeanId(Organization.TYPE, org));
						}
					}
				}
			}
		} while (repeat);

		for (OneRosterBean bean : m_allBeans.values()) {
			ThreadUtils.checkInterrupt();

			if (bean instanceof com.follett.cust.io.exporter.oneroster.v1_1.Class
					&& requiredNodes.contains(bean.getBeanId())) {
				com.follett.cust.io.exporter.oneroster.v1_1.Class c = (com.follett.cust.io.exporter.oneroster.v1_1.Class) bean;

				requiredNodes.add(
						new BeanId(Organization.TYPE, c.getSchoolSourcedId()));
				if (c.getCourseSourcedId() != null) {
					requiredNodes.add(
							new BeanId(Course.TYPE, c.getCourseSourcedId()));
				}

				List<String> terms = c.getTermSourcedIds();
				if (terms != null) {
					for (String term : terms) {
						requiredNodes
								.add(new BeanId(AcademicSession.TYPE, term));
					}
				}
			}
		}

		for (OneRosterBean bean : m_allBeans.values()) {
			ThreadUtils.checkInterrupt();

			if (bean instanceof Course
					&& requiredNodes.contains(bean.getBeanId())) {
				Course c = (Course) bean;

				if (c.getSchoolYearId() != null) {
					requiredNodes.add(new BeanId(AcademicSession.TYPE,
							c.getSchoolYearId()));
				}
				if (c.getOrgSourcedId() != null) {
					requiredNodes.add(
							new BeanId(Organization.TYPE, c.getOrgSourcedId()));
				}
			}
		}

		do {
			repeat = false;
			for (OneRosterBean bean : m_allBeans.values()) {
				ThreadUtils.checkInterrupt();
				if (bean instanceof AcademicSession
						&& requiredNodes.contains(bean.getBeanId())) {
					AcademicSession as = (AcademicSession) bean;

					if (as.getParentSourcedId() != null) {
						if (requiredNodes.add(new BeanId(AcademicSession.TYPE,
								as.getParentSourcedId()))) {
							repeat = true;
						}
					}
				}
			}
		} while (repeat);

		do {
			repeat = false;
			for (OneRosterBean bean : m_allBeans.values()) {
				ThreadUtils.checkInterrupt();
				if (bean instanceof Organization
						&& requiredNodes.contains(bean.getBeanId())) {
					Organization org = (Organization) bean;

					if (org.getParentSourcedId() != null) {
						if (requiredNodes.add(new BeanId(Organization.TYPE,
								org.getParentSourcedId()))) {
							repeat = true;
						}
					}
				}
			}
		} while (repeat);

		for (OneRosterBean bean : m_allBeans.values()) {
			ThreadUtils.checkInterrupt();
			if (bean instanceof Demographics) {
				Demographics d = (Demographics) bean;
				BeanId userId = new BeanId(User.TYPE, d.getSourcedId());
				if (m_allBeans.containsKey(userId)) {
					requiredNodes.add(d.getBeanId());
				}
			}
		}

		for (OneRosterBean bean : m_allBeans.values()) {
			ThreadUtils.checkInterrupt();
			if (bean instanceof LineItem) {
				LineItem l = (LineItem) bean;
				BeanId categoryId = new BeanId(Category.TYPE,
						l.getCategorySourcedId());
				BeanId classId = new BeanId(
						com.follett.cust.io.exporter.oneroster.v1_1.Class.TYPE,
						l.getClassSourcedId());
				BeanId sessionId = new BeanId(AcademicSession.TYPE,
						l.getGradingPeriodSourcedId());
				if (requiredNodes.contains(sessionId)
						&& requiredNodes.contains(classId)) {
					requiredNodes.add(bean.getBeanId());
					requiredNodes.add(categoryId);
				}
			}
		}

		for (OneRosterBean bean : m_allBeans.values()) {
			ThreadUtils.checkInterrupt();
			if (bean instanceof Result) {
				Result r = (Result) bean;
				BeanId userId = new BeanId(User.TYPE, r.getStudentSourcedId());
				BeanId lineItemId = new BeanId(LineItem.TYPE,
						r.getLineItemSourcedId());
				if (requiredNodes.contains(userId)
						&& requiredNodes.contains(lineItemId)) {
					requiredNodes.add(bean.getBeanId());
				}
			}
		}

		// we've cataloged everything we know is of interest.
		// now we can prune the rest:

		boolean returnValue = false;
		Iterator<Entry<BeanId, OneRosterBean>> iter = m_allBeans.entrySet()
				.iterator();
		while (iter.hasNext()) {
			ThreadUtils.checkInterrupt();
			Entry<BeanId, OneRosterBean> entry = iter.next();
			if (!requiredNodes.contains(entry.getKey())) {
				OneRosterBean bean = entry.getValue();
				if (bean instanceof User) {
					RoleType role = ((User) bean).getRole();
					if (RoleType.ADMINISTRATOR == role || RoleType.AIDE == role
							|| RoleType.PROCTOR == role) {
						continue;
					}
				}
				returnValue = true;
				m_exceptionHandler.handlePrunedBean(bean);
				iter.remove();
			}
		}

		return returnValue;
	}

	/**
	 * Save one or more beans to the {@link #m_allBeans} map. This may also
	 * validate the bean fields (not the relationships), and this may throw a
	 * RuntimeException if bad fields are identified.
	 *
	 * @param result
	 *            the optional RowResult that helped create the OneRosterBeans.
	 * @param throwErrorForUsedUid
	 *            if true then this will throw an exception if the bean ID is
	 *            already in use. For example: if the user has requested to use
	 *            local IDs of users and staff as the UID for the OneRosterBean:
	 *            we need to throw an exception if the same ID is being used for
	 *            two different people.
	 * @param beans
	 *            the beans to save.
	 */
	protected void saveBean(RowResult result, boolean throwErrorForUsedUid,
			OneRosterBean... beans) throws RecordLimitException {
		List<RuntimeException> exceptions = new ArrayList<>();
		for (OneRosterBean bean : beans) {
			try {
				if (m_strictFieldValidation) {
					bean.validateFields();
				}
				if (m_allBeans.size() >= getLimit()) {
					String str = NumberFormat.getInstance().format(getLimit());

					String schoolOid = (String) getParameter(PARAM_SCHOOL_OID);
					if (StringUtils.isEmpty(schoolOid)) {
						throw new RecordLimitException(
								"This export is creating too many One Roster records (over "
										+ str
										+ "). Please narrow the schools involved in this export and try again.");
					}
					throw new RecordLimitException(
							"This export is creating too many One Roster records (over "
									+ str
									+ "). Please narrow the constraints for this export or contact technical support.");
				}
				Object oldRecord = m_allBeans.get(bean.getBeanId());
				if (throwErrorForUsedUid && oldRecord != null) {
					throw new RuntimeException("The ID \"" + bean.getBeanId()
							+ "\" is used by multiple records.\n" + oldRecord
							+ "\n" + bean);
				}
				m_allBeans.put(bean.getBeanId(), bean);
			} catch (RuntimeException e) {
				exceptions.add(e);
			}
		}
		if (exceptions.size() == 1) {
			throw exceptions.get(0);
		}
		if (exceptions.size() > 1) {
			throw new MultipleRuntimeException(exceptions);
		}
	}

	/**
	 * The maximum number of beans this export will save. If we exceed this
	 * amount an exception is thrown. The recommended default value here is
	 * 1,000,000.
	 * <p>
	 * This is an arbitrary limit, but please don't remove it entirely if you're
	 * dealing with a large school. Remember if this export fails with a memory
	 * exception: it may bring down the rest of the VM with it.
	 */
	private int getLimit() {
		Number n = (Number) getParameter(PARAM_LIMIT);
		if (n == null) {
			n = Integer.valueOf(1);
		}
		int limit = (int) (1000000 * n.doubleValue());

		return limit;
	}

	/**
	 * Save a Demographics record that includes the birthdate and gender of a
	 * User.
	 *
	 * @param user
	 *            the user to write the demographics record for.
	 * @param birthdate
	 *            the optional birthdate
	 * @param genderCode
	 *            the optional gender code (as defined in Aspen)
	 *
	 * @return true if a Demographics bean is saved, false otherwise.
	 */
	protected boolean saveUserDemographic(User user, Date birthdate,
			String genderCode) throws RecordLimitException {
		if (birthdate == null && StringUtils.isEmpty(genderCode)) {
			return false;
		}
		Demographics demographics = new Demographics(user.getSourcedId());
		demographics.setBirthDate(birthdate);
		if (genderCode != null && genderCode.toUpperCase().startsWith("M")) {
			demographics.setSex(Gender.MALE);
		} else if (genderCode != null
				&& genderCode.toUpperCase().startsWith("F")) {
			demographics.setSex(Gender.FEMALE);
		}
		saveBean(null, true, demographics);
		return true;
	}

	/**
	 * Add an agent to a user.
	 *
	 * @param user
	 *            the user to add the agent to.
	 * @param otherUserUid
	 *            the id to add to the user's list of agents.
	 */
	private void addAgent(User user, String otherUserUid) {
		List<String> orgs = user.getAgents();
		if (orgs == null) {
			orgs = new ArrayList<>();
		}
		orgs.add(otherUserUid);
		user.setAgents(orgs);
	}

	/**
	 * Add an organization to a user.
	 *
	 * @param user
	 *            the user to add the organization to.
	 * @param schoolId
	 *            the id to add to the user's list of organizations.
	 */
	private void addOrg(User user, String schoolId) {
		List<String> orgs = user.getOrgSourcedIds();
		if (orgs == null) {
			orgs = new ArrayList<>();
		}
		orgs.add(schoolId);
		user.setOrgSourcedIds(orgs);
	}

	private Collection<String> listCodes(String... codes) {
		Collection<String> returnValue = new HashSet<>();
		for (String code : codes) {
			returnValue.add(strip(code));
		}

		return returnValue;
	}

	private String strip(String value) {
		if (value == null) {
			return null;
		}
		StringBuffer sb = new StringBuffer();
		for (int a = 0; a < value.length(); a++) {
			char ch = value.charAt(a);
			if (Character.isLetter(ch)) {
				sb.append(Character.toLowerCase(ch));
			}
		}

		return sb.toString();
	}

	/**
	 * Convert a Collection into a comma-separated String.
	 *
	 * @param c
	 *            the collection to convert.
	 *
	 * @return String a comma-separated series of the elements in c.
	 */
	private String toString(Collection c) {
		StringBuffer sb = new StringBuffer();
		for (Object t : c) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(String.valueOf(t));
		}

		return sb.toString();
	}
}