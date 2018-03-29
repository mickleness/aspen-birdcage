package org.abc.tools.reports;

/*
 * ====================================================================
 *
 * X2 Development Corporation
 *
 * Copyright (c) 2002-2003 X2 Development Corporation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, is not permitted without a written agreement
 * from X2 Development Corporation.
 *
 * ====================================================================
 */

import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import net.sf.jasperreports.engine.JRDataSource;

import org.abc.tools.Tool;
import org.abc.util.GroupedMapIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;

import com.follett.cust.tools.JasperDataPublisher;
import com.follett.fsc.core.framework.persistence.SubQuery;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.Report;
import com.follett.fsc.core.k12.beans.StaffSchoolAssociation;
import com.follett.fsc.core.k12.beans.SystemPreferenceDefinition;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.EmailAttachmentDataSource;
import com.follett.fsc.core.k12.business.MessageProperties;
import com.follett.fsc.core.k12.business.PreferenceManager;
import com.follett.fsc.core.k12.business.WriteEmailManager;
import com.follett.fsc.core.k12.tools.ToolInput;
import com.follett.fsc.core.k12.tools.ToolJob;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.sis.model.beans.SisAddress;
import com.x2dev.sis.model.beans.SisSchool;
import com.x2dev.sis.model.beans.SisStaff;
import com.x2dev.sis.model.beans.StaffCertification;
import com.x2dev.sis.model.beans.path.SisBeanPaths;
import com.x2dev.utils.CollectionUtils;
import com.x2dev.utils.LoggerUtils;
import com.x2dev.utils.X2BaseException;

/**
 * Prepares the data for the following staff reports:
 * <ul>
 * <li>Staff Directory
 * <li>Staff List
 * <li>Staff Labels
 * </ul>
 * These reports simply select staff from the current school and order the
 * results by last name or department.
 * <p>
 * This is an optional input parameter that controls whether this report
 * automatically emails the report to staff.
 *
 * @author X2 Development Corporation
 */
@Tool(id = "STF-VER-RPT", name = "Staff Verification Info Report", category = "Staff", comment = "Runs at district", weight = "1", schedulable = "false", jrxml = "StaffVerificationInfoReport.jrxml", input = "StaffVerificationInfoReportInput.xml", nodes = {
		"key=\"staff.staff.list\" org1-view=\"true\" personnel-view=\"true\" school-view=\"true\" staff-view=\"true\"",
		"key=\"staff.staff.list.detail\" org1-view=\"true\" personnel-view=\"true\" school-view=\"true\"",
		"key=\"mystuff.myrecord.staff.detail\" staff-view=\"true\"" })
public class StaffListData extends ReportJavaSourceNet {
	/**
     * 
     */
	private static final long serialVersionUID = 1L;

	/**
	 * Name for the "active only" report parameter. The value is a Boolean.
	 */
	public static final String ACTIVE_ONLY_PARAM = "activeOnly";

	/**
	 * Name for the "group by school" report parameter. The value is a Boolean.
	 */
	public static final String GROUP_BY_SCHOOL_PARAM = "groupBySchool";

	/**
	 * Name for the "Email Staff" report parameter. The value is a Boolean.
	 */
	public static final String EMAIL_PARAM = "emailPdf";

	/**
	 * Name for the enumerated "selection" report parameter. The value is an
	 * Integer.
	 */
	public static final String QUERY_BY_PARAM = "queryBy";

	/**
	 * Name for the "secondary staff" report parameter. The value is a Boolean.
	 */
	public static final String SECONDARY_STAFF_PARAM = "secondaryStaff";

	/**
	 * Name for the "sort" report parameter. The value is an Integer.
	 */
	public static final String SORT_PARAM = "sort";

	private SisStaff m_currentStaff;

	/** Used for emails. */
	String ownerOid;
	/** Used for emails. */
	int ownerType;

	/**
	 * @see com.follett.fsc.core.k12.tools.reports.ReportJavaSourceDori#gatherData()
	 */
	@Override
	protected JRDataSource gatherData() {
		ReportDataGrid grid = new ReportDataGrid();

		QueryByCriteria query = createQueryByCriteria(SisStaff.class,
				buildCriteria());
		buildSortOrder(query);

		QueryIterator iterator = getBroker().getIteratorByQuery(query);
		try {
			while (iterator.hasNext()) {
				SisStaff staff = (SisStaff) iterator.next();

				SisAddress address = staff.getPerson().getPhysicalAddress();

				Collection<StaffCertification> certifications = staff
						.getCertifications(getBroker());
				if (!CollectionUtils.isEmpty(certifications)) {
					for (StaffCertification certification : certifications) {
						grid.append();
						grid.set("staff", staff);
						grid.set("license", certification);
						grid.set("expirationDate",
								certification.getExpirationDate());
						grid.set("nameView", staff.getNameView());
					}
				} else {
					grid.append();
					grid.set("staff", staff);
					grid.set("nameView", staff.getNameView());
				}
			}
		} finally {
			iterator.close();
		}

		List<String> sort = new ArrayList<String>();
		List<Boolean> orders = new ArrayList<Boolean>();

		sort.add("nameView");
		orders.add(Boolean.TRUE);

		sort.add("expirationDate");
		orders.add(Boolean.FALSE);

		grid.sort(sort, orders, false);

		grid.beforeTop();
		return grid;
	}

	/**
	 * Build the criteria based on user input.
	 * 
	 * @return Criteria
	 */
	private Criteria buildCriteria() {
		Criteria criteria = new Criteria();

		String queryBy = (String) getParameter(QUERY_BY_PARAM);
		if (queryBy.equals(SELECTION_SPECIAL_CASE_PREFIX + CURRENT_KEY)) {
			criteria = getCurrentCriteria();
		} else {
			if (isSchoolContext()) {
				criteria.addEqualTo(SisStaff.COL_SCHOOL_OID, getSchool()
						.getOid());

				if (((Boolean) getParameter(SECONDARY_STAFF_PARAM))
						.booleanValue()) {
					criteria.addOrCriteria(getSecondaryCriteria());
				}
			} else {
				criteria.addEqualTo(SisStaff.COL_ORGANIZATION1_OID,
						getOrganization().getOid());
			}
		}

		if (((Boolean) getParameter(ACTIVE_ONLY_PARAM)).booleanValue()) {
			String activeCode = PreferenceManager.getPreferenceValue(
					getOrganization(),
					SystemPreferenceDefinition.STAFF_ACTIVE_CODE);
			criteria.addEqualTo(SisStaff.COL_STATUS, activeCode);
		}

		if (m_currentStaff != null) {
			criteria.addEqualTo(X2BaseBean.COL_OID, m_currentStaff.getOid());
		}

		return criteria;
	}

	@Override
	protected void saveState(UserDataContainer userData) throws X2BaseException {
		// TODO Auto-generated method stub
		m_currentStaff = (SisStaff) userData.getCurrentRecord(SisStaff.class);
		ownerType = userData.getCurrentOwnerType();
		ownerOid = userData.getCurrentOwnerOid();
		super.saveState(userData);
	}

	/**
	 * Updates the passed query with sorting based on user input.
	 * 
	 * @param query
	 */
	private void buildSortOrder(QueryByCriteria query) {
		/*
		 * Sort by the school first if the report is grouped by school. We add
		 * the school OID as a sort parameter just in case two schools have the
		 * same name (unlikely but possible).
		 */
		if (((Boolean) getParameter(GROUP_BY_SCHOOL_PARAM)).booleanValue()
				&& !isSchoolContext()) {
			query.addOrderByAscending(SisStaff.REL_SCHOOL + PATH_DELIMITER
					+ SisSchool.COL_NAME);
			query.addOrderByAscending(SisStaff.COL_SCHOOL_OID);

			/*
			 * Staff aren't necessarily associated with a school. We don't want
			 * the sort to eliminate records (see ticket S10010842).
			 */
			query.setPathOuterJoin(SisStaff.REL_SCHOOL);
		}

		/*
		 * Now sort the staff members based on the user's selection.
		 */
		applyUserSort(query, (String) getParameter(SORT_PARAM));
	}

	/**
	 * Builds criteria to get Staff with a secondary association with the
	 * current school.
	 * 
	 * @return Criteria
	 */
	private Criteria getSecondaryCriteria() {
		Criteria criteria = new Criteria();

		Criteria secondaryCriteria = new Criteria();
		secondaryCriteria.addEqualTo(StaffSchoolAssociation.COL_SCHOOL_OID,
				getSchool().getOid());
		secondaryCriteria.addEqualTo(
				StaffSchoolAssociation.COL_DISTRICT_CONTEXT_OID,
				getOrganization().getCurrentContextOid());

		SubQuery secondarySub = new SubQuery(StaffSchoolAssociation.class,
				StaffSchoolAssociation.COL_STAFF_OID, secondaryCriteria);

		criteria.addIn(X2BaseBean.COL_OID, secondarySub);

		return criteria;
	}

	@Override
	protected void publishResults() throws Exception {
		if (((Boolean) getParameter(EMAIL_PARAM)).booleanValue()) {
			emailPdfs();
		}
		super.publishResults();
	}

	protected void emailPdfs() {
		ReportDataGrid data = (ReportDataGrid) getDataSource();
		GroupedMapIterator iter = new GroupedMapIterator(
				(Iterable) data.getRows(), "staff");
		ToolJob job = getJob();
		Report report = (Report) job.getTool();

		try {
			JasperDataPublisher.Configuration configuration = new JasperDataPublisher.Configuration(
					getBroker(), report.getEngineVersion(), getFormat(), null,
					ToolInput.PDF_FORMAT, job);

			WriteEmailManager emailManager = new WriteEmailManager(
					getOrganization(), ownerOid, ownerType, getUser());
			if (emailManager.connect()) {
				try {
					while (iter.hasNext()) {
						List<Map> rows = iter.next();
						SisStaff staff = (SisStaff) rows.get(0).get("staff");
						String emailAddress = (String) staff
								.getFieldValueByBeanPath(SisBeanPaths.STAFF
										.person().email01().toString());
						if (!StringUtils.isEmpty(emailAddress)) {
							try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
								try (JasperDataPublisher publisher = new JasperDataPublisher(
										configuration, out)) {
									for (Map row : rows) {
										publisher.publish(row);
									}
								}
								emailFile(emailManager, emailAddress,
										"Attached is a PDF generated by "
												+ job.getTool().getId(), job
												.getTool().getId() + " Report",
										staff.getNameView() + ".pdf",
										out.toByteArray());
							}
						}
					}
				} finally {
					emailManager.disconnect();
				}
			}
		} catch (Exception e) {
			String msg = LoggerUtils.convertThrowableToString(e);
			this.logToolMessage(Level.SEVERE, msg, false);
		}
	}

	private void emailFile(WriteEmailManager emailManager, String emailAddress,
			String messageBody, String messageSubject, String filename,
			byte[] fileData) {
		List<EmailAttachmentDataSource> attachments = new ArrayList<>();
		attachments.add(new EmailAttachmentDataSource(fileData, filename,
				"application/pdf"));
		MessageProperties messageProperties = new MessageProperties(
				Arrays.asList(emailAddress), null, null, null, messageSubject,
				messageBody, "text/plain", null, null, attachments);

		emailManager.sendMail(messageProperties);
	}
}