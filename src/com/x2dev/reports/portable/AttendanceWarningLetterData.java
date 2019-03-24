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

package com.x2dev.reports.portable;

import static com.follett.fsc.core.k12.beans.SystemPreferenceDefinition.STUDENT_ACTIVE_CODE;
import com.follett.fsc.core.framework.persistence.ColumnQuery;
import com.follett.fsc.core.k12.beans.OrganizationLocale;
import com.follett.fsc.core.k12.beans.Report;
import com.follett.fsc.core.k12.beans.ReportQueryIterator;
import com.follett.fsc.core.k12.beans.Student;
import com.follett.fsc.core.k12.beans.StudentContact;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.ModelBroker;
import com.follett.fsc.core.k12.business.PreferenceManager;
import com.follett.fsc.core.k12.business.localization.LocalizationCache;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.follett.fsc.core.k12.tools.reports.ReportUtils;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.follett.fsc.core.k12.web.ContextList;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.sis.model.beans.SisDataFieldConfig;
import com.x2dev.sis.model.beans.SisOrganization;
import com.x2dev.sis.model.beans.SisSchool;
import com.x2dev.sis.model.beans.SisStudent;
import com.x2dev.sis.model.beans.StudentAttendance;
import com.x2dev.sis.model.beans.StudentEventTracking;
import com.x2dev.utils.converters.Converter;
import com.x2dev.utils.converters.ConverterFactory;
import com.x2dev.utils.types.PlainDate;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import net.sf.jasperreports.engine.JRDataSource;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;
import org.apache.struts.util.MessageResources;

/**
 * Prepares the data for PORATABLE REPORTS PROJECT "Attendance Warning Letter" report. This report
 * lists the
 * student attendance matching the total criteria within the entered date range. It also creates
 * StudentEventTracking records for the students receiving letters.
 * <p>
 * This report takes the following consideration in the SQL based on the wording of the letter
 * are:
 * <ul>
 * <li>SQL will always query for students who have greater than the parameterized absences
 * (irrespective of Student Event records)
 * </ul>
 *
 * @author Follett School Solutions
 */
public class AttendanceWarningLetterData extends ReportJavaSourceNet {
    /**
     * Name for the "end date" report parameter. The value is a PlainDate.
     */
    public static final String END_DATE_PARAM = "endDate";

    /**
     * Name for the "exclude excused absences" report parameter. The value is a Boolean.
     */
    public static final String EXCLUDE_EXCUSED_PARAM = "excludeExcused";

    /**
     * Name for the "include students with previous mailing" report parameter. The value is a
     * Boolean.
     */
    public static final String INCLUDE_ALL_PARAM = "includeAll";

    /**
     * Name for the "include students with previous mailing" report parameter. The value is a
     * Boolean.
     */
    public static final String GENERATE_EVENTS_PARAM = "generateEvents";

    /**
     * Name for the "Minimum absences" report parameter. The value is an integer.
     */
    public static final String MINIMUM_ABSENCES_PARAM = "minimumAbsences";

    /**
     * Name for the "multiple mailings" report parameter. The value is a Boolean.
     */
    public static final String MULTIPLE_MAILINGS_PARAM = "multipleMailings";

    /**
     * Name for the "sort" report parameter. The value is an Integer.
     */
    public static final String SORT_PARAM = "sort";

    /**
     * Name for the "start date" report parameter. The value is a PlainDate.
     */
    public static final String START_DATE_PARAM = "startDate";

    /**
     * Name for the "eventComment" report parameter. The value is a String.
     */
    public static final String EVENT_COMMENT_PARAM = "eventComment";

    /**
     * Name for the "eventPrefix" parameter. Value is a String.
     */
    public static final String EVENT_NAME_PARAM = "eventPrefix";

    /**
     * Name for the "numbersToWords" parameter for portability. Value is a String.
     */
    public static final String NUMBERS_TO_WORDS_PARAM = "numbersToWords";

    /**
     * Name for the "includeChartBar" parameter. Value is a boolean.
     */
    public static final String PRINT_CHART_BAR_PARAM = "printChartBar";

    /**
     * Name for the "categories" parameter. Value is a String.
     */
    public static final String CHART_BAR_CATEGORIES_PARAM = "categories";

    /**
     * Name for the "series" parameter. Value is a String.
     */
    public static final String CHART_BAR_SERIES_PARAM = "series";

    // private static final String EVENT_PREFIX = "Attendance Failure Warning Letter - 4 Days";

    /**
     * Grid fields
     */
    public static final String COL_ABSENT_TOTAL = "absences";
    public static final String COL_ADDRESS = "address";
    public static final String COL_STUDENT = "student";
    public static final String COL_PRINT_CHART = "printChartBar";
    public static final String COL_CHART_FORMAT = "chartFormat";
    public static final String COL_CHART_DATA = "chartData";
    public static final String COL_SUMMARY_FORMAT = "summaryFormat";
    public static final String COL_SUMMARY_DATA = "summaryData";

    /**
     * Chart Grid fields
     */
    public static final String COL_CATEGORY = "categoryField";
    public static final String COL_SERIES = "seriesField";
    public static final String COL_COUNT = "countField";
    public static final String COL_TOTAL_COUNT = "totalCount";

    /**
     * Grid parameters
     */
    public static final String CATEGORY = "category";
    public static final String SERIES = "series";

    /**
     * Sub Report IDs
     */
    public static final String SUB_CHART_REPORT_ID = "FSS-ATT-008-SUB-C";
    public static final String SUB_CHART_SUMMARY_REPORT_ID = "FSS-ATT-008-SUB-S";

    // m_variables
    private Map m_studentContacts;
    private Map m_studentEvents;
    private String m_studentOid;
    private String m_eventName;

    /**
     * arrays used to convert numbers into English word for future portability reasons
     */
    private static final String[] TENS_NAMES =
            {
                    "",
                    " ten",
                    " twenty",
                    " thirty",
                    " forty",
                    " fifty",
                    " sixty",
                    " seventy",
                    " eighty",
                    " ninety"
            };

    private static final String[] NUM_NAMES =
            {
                    "",
                    " one",
                    " two",
                    " three",
                    " four",
                    " five",
                    " six",
                    " seven",
                    " eight",
                    " nine",
                    " ten",
                    " eleven",
                    " twelve",
                    " thirteen",
                    " fourteen",
                    " fifteen",
                    " sixteen",
                    " seventeen",
                    " eighteen",
                    " nineteen"
            };

    /**
     * @see com.follett.fsc.core.k12.tools.reports.ReportJavaSourceDori#gatherData()
     *      Localization Constants and Variables
     */
    Map<String, String> m_validLocales;
    private static final String AMERICAN_ENGLISH_LOCALE = "en_US";
    private static final String LOCALES = "locales";
    private String m_defaultLocale;
    public String DEFAULT_LOCALE_PARAM = "default_locale";

    /**
     * @see com.x2dev.sis.tools.reports.ReportJavaSourceDori#gatherData()
     */
    @Override
    protected JRDataSource gatherData() {
        ReportDataGrid grid = new ReportDataGrid(300, 2);
        ReportDataGrid chartGrid = new ReportDataGrid();
        ReportDataGrid summaryGrid = new ReportDataGrid();

        /*
         * Initializing localized parameters and values to allow for custom report sections
         */
        initializeLocalized();

        /*
         * Prepare the SQL parameters based on preferences/user input.
         */
        String activeCode = PreferenceManager.getPreferenceValue(getOrganization(), STUDENT_ACTIVE_CODE);
        int minimumAbsences = ((Integer) getParameter(MINIMUM_ABSENCES_PARAM)).intValue();
        BigDecimal lowerBound = new BigDecimal(minimumAbsences);
        PlainDate endDate = (PlainDate) getParameter(END_DATE_PARAM);
        boolean excludeExcused = ((Boolean) getParameter(EXCLUDE_EXCUSED_PARAM)).booleanValue();
        boolean includeAll = ((Boolean) getParameter(INCLUDE_ALL_PARAM)).booleanValue();
        boolean generateEvents = ((Boolean) getParameter(GENERATE_EVENTS_PARAM)).booleanValue();
        PlainDate startDate = (PlainDate) getParameter(START_DATE_PARAM);
        boolean multipleMailings = ((Boolean) getParameter(MULTIPLE_MAILINGS_PARAM)).booleanValue();
        String eventComment = getParameter(EVENT_COMMENT_PARAM).toString();
        m_eventName = getParameter(EVENT_NAME_PARAM).toString();

        boolean printChartBar = ((Boolean) getParameter(PRINT_CHART_BAR_PARAM)).booleanValue();
        String categoryFieldOid = (String) getParameter(CHART_BAR_CATEGORIES_PARAM);
        String seriesFieldOid = (String) getParameter(CHART_BAR_SERIES_PARAM);

        String eventType = m_eventName; // + lowerBound;

        loadStudentEvents();

        /*
         * Execute a SQL statement to get the students and their absence total. The SQL is easier
         * to write/maintain even though a query is then run for every student that matches the
         * result set.
         */
        if (startDate != null && endDate != null && endDate.after(startDate)) {
            StringBuilder sql = new StringBuilder(512);

            sql.append(
                    "SELECT A.ATT_STD_OID, SUM(A.ATT_PORTION_ABSENT), T0.TRK_OID, STD_NAME_VIEW, STD_YOG, STD_HOMEROOM");
            sql.append("  FROM (");

            sql.append("   SELECT ATT_STD_OID, ATT_PORTION_ABSENT");
            sql.append("     FROM STUDENT_ATTENDANCE");
            sql.append("    INNER JOIN STUDENT ON ATT_STD_OID = STD_OID");
            sql.append("    WHERE ATT_DATE >= ?");
            sql.append("      AND ATT_DATE <= ?");
            sql.append("      AND STD_ENROLLMENT_STATUS = '" + activeCode + "'");
            sql.append("      AND STD_SKL_OID = '" + ((SisSchool) getSchool()).getOid() + "'");

            if (m_studentOid != null) {
                sql.append("      AND STD_OID = '" + m_studentOid + "'");
            }

            if (excludeExcused) {
                sql.append("      AND ATT_EXCUSED_IND = '0'");
            }

            sql.append(") A INNER JOIN STUDENT ON A.ATT_STD_OID = STD_OID ");
            sql.append("    LEFT OUTER JOIN STUDENT_EVENT_TRACKING T0 ON T0.TRK_STD_OID = STD_OID ");
            sql.append("                                             AND T0.TRK_CTX_OID = '"
                    + ((SisOrganization) getOrganization()).getCurrentContextOid() + "'");
            sql.append("                                             AND T0.TRK_EVENT_TYPE = '" + eventType + " >"
                    + lowerBound.doubleValue() + " days'");
            sql.append(" GROUP BY A.ATT_STD_OID, T0.TRK_OID, STD_NAME_VIEW, STD_YOG, STD_HOMEROOM ");
            sql.append("HAVING SUM(A.ATT_PORTION_ABSENT) > " + lowerBound.doubleValue());
            sql.append(" ORDER BY ");

            int sort = ((Integer) getParameter(SORT_PARAM)).intValue();
            switch (sort) {
                case 1: // YOG
                    sql.append("5, 4");
                    break;

                case 2: // Homeroom
                    sql.append("6, 4");
                    break;

                default: // Name (case 0)
                    sql.append("4");
                    break;
            }

            addParameter(NUMBERS_TO_WORDS_PARAM, numbersToWords(minimumAbsences));

            Connection connection = getBroker().borrowConnection();
            PreparedStatement statement = null;
            ResultSet results = null;

            if (multipleMailings) {
                loadMailingContacts();
            }

            try {
                ModelBroker broker = new ModelBroker(getPrivilegeSet());
                PlainDate today = new PlainDate();

                statement = connection.prepareStatement(sql.toString());
                statement.setDate(1, startDate);
                statement.setDate(2, endDate);

                results = statement.executeQuery();
                while (results.next()) {
                    String studentOid = results.getString(1);
                    double absentTotal = results.getDouble(2);

                    SisStudent student = (SisStudent) getBroker().getBeanByOid(Student.class, studentOid);

                    StudentEventTracking event = (StudentEventTracking) m_studentEvents.get(studentOid);

                    if (event == null || includeAll) {
                        /*
                         * Add the student bean the absence total to a grid. Create a event record
                         * if
                         * one doesn't already exist.
                         */
                        grid.append();
                        grid.set(COL_STUDENT, student);
                        grid.set(COL_ABSENT_TOTAL, new Double(absentTotal));
                        grid.set(COL_ADDRESS, student.getPerson().getResolvedMailingAddress());
                        grid.set(COL_PRINT_CHART, Boolean.valueOf(printChartBar));

                        if (printChartBar) {
                            SisDataFieldConfig categoryFieldConfig =
                                    (SisDataFieldConfig) getBroker().getBeanByOid(SisDataFieldConfig.class,
                                            categoryFieldOid);
                            SisDataFieldConfig seriesFieldConfig =
                                    (SisDataFieldConfig) getBroker().getBeanByOid(SisDataFieldConfig.class,
                                            seriesFieldOid);

                            addParameter(CATEGORY, categoryFieldConfig.getUserLongName());

                            if (seriesFieldConfig != null) {
                                addParameter(SERIES, seriesFieldConfig.getUserLongName());
                            }

                            Criteria attendanceCriteria = buildAttendanceCriteria(student, startDate, endDate);
                            ColumnQuery chartQuery =
                                    buildAttendanceQuery(categoryFieldOid, seriesFieldOid, attendanceCriteria);
                            chartGrid = populateChartGrid(chartQuery);

                            Report chartFormat = ReportUtils.getReport(SUB_CHART_REPORT_ID, getBroker());

                            grid.set(COL_CHART_FORMAT, new ByteArrayInputStream(chartFormat.getCompiledFormat()));
                            grid.set(COL_CHART_DATA, chartGrid);

                            summaryGrid = populateChartGrid(chartQuery);

                            Report summaryFormat = ReportUtils.getReport(SUB_CHART_SUMMARY_REPORT_ID, getBroker());

                            grid.set(COL_SUMMARY_FORMAT, new ByteArrayInputStream(summaryFormat.getCompiledFormat()));
                            grid.set(COL_SUMMARY_DATA, summaryGrid);
                        }

                        if (multipleMailings) {
                            Collection contacts = (Collection) m_studentContacts.get(studentOid);
                            if (contacts != null) {
                                Iterator contactIterator = contacts.iterator();
                                while (contactIterator.hasNext()) {
                                    StudentContact contact = (StudentContact) contactIterator.next();

                                    grid.append();
                                    grid.set(COL_STUDENT, student);
                                    grid.set(COL_ABSENT_TOTAL, new Double(absentTotal));
                                    grid.set(COL_ADDRESS,
                                            contact.getContact().getPerson().getResolvedMailingAddress());
                                    grid.set(COL_PRINT_CHART, Boolean.valueOf(printChartBar));

                                    if (printChartBar) {
                                        Report chartFormat = ReportUtils.getReport(SUB_CHART_REPORT_ID, getBroker());

                                        grid.set(COL_CHART_FORMAT,
                                                new ByteArrayInputStream(chartFormat.getCompiledFormat()));
                                        grid.set(COL_CHART_DATA, chartGrid);

                                        Report summaryFormat =
                                                ReportUtils.getReport(SUB_CHART_SUMMARY_REPORT_ID, getBroker());

                                        grid.set(COL_SUMMARY_FORMAT,
                                                new ByteArrayInputStream(summaryFormat.getCompiledFormat()));
                                        grid.set(COL_SUMMARY_DATA, summaryGrid);
                                    }
                                }
                            }
                        }

                        // Should not create an event if includeAll was checked off.
                        if (generateEvents) {
                            if (event == null) {
                                StudentEventTracking newEvent = X2BaseBean.newInstance(StudentEventTracking.class,
                                        getBroker().getPersistenceKey());

                                newEvent.setDistrictContextOid(
                                        ((SisOrganization) getOrganization()).getCurrentContextOid());
                                newEvent.setEventDate(today);
                                newEvent.setEventType(m_eventName + " >" + minimumAbsences + " days");
                                newEvent.setStudentOid(studentOid);
                                newEvent.setComment(eventComment + " --> Report run for date range: " + startDate
                                        + " to " + endDate);

                                broker.saveBean(newEvent);
                            }
                        }

                    }
                }
            } catch (SQLException sqle) {
                AppGlobals.getLog().log(Level.WARNING, sqle.getMessage(), sqle);
            } finally {
                try {
                    if (results != null) {
                        results.close();
                    }

                    if (statement != null) {
                        statement.close();
                    }
                } catch (Exception e) {
                    AppGlobals.getLog().log(Level.WARNING, e.getMessage(), e);
                }

                getBroker().returnConnection();
            }
        }

        grid.beforeTop();

        return grid;
    }

    /**
     * Remember the currently selected student if this report is being run from the student module.
     *
     * @see com.x2dev.sis.tools.ToolJavaSource#saveState(com.x2dev.sis.web.UserDataContainer)
     */
    @Override
    protected void saveState(UserDataContainer userData) {
        ContextList parentList = userData.getParentList();
        if (parentList != null && parentList.getDataClass().equals(SisStudent.class)) {
            m_studentOid = parentList.getCurrentRecord().getOid();
        }
    }

    /**
     * Loads the mailing contacts for students into a Map of StudentContacts keyed to student OID.
     */
    private void loadMailingContacts() {
        Criteria criteria = new Criteria();
        criteria.addEqualTo(StudentContact.COL_CONDUCT_MAILING_INDICATOR, Boolean.TRUE);
        criteria.addNotEqualTo(StudentContact.COL_LIVES_WITH_INDICATOR, Boolean.TRUE);

        if (isSchoolContext()) {
            criteria.addEqualTo(StudentContact.REL_STUDENT + "." + SisStudent.COL_SCHOOL_OID,
                    ((SisSchool) getSchool()).getOid());
        }

        QueryByCriteria query = new QueryByCriteria(StudentContact.class, criteria);
        m_studentContacts = getBroker().getGroupedCollectionByQuery(query, StudentContact.COL_STUDENT_OID, 2000);
    }

    /**
     * Loads the student event records into a Map keyed to student OID.
     */
    private void loadStudentEvents() {
        Criteria criteria = new Criteria();
        criteria.addEqualTo(StudentEventTracking.COL_DISTRICT_CONTEXT_OID,
                ((SisOrganization) getOrganization()).getCurrentContextOid());
        criteria.addLike(StudentEventTracking.COL_EVENT_TYPE, m_eventName + "%");

        QueryByCriteria query = new QueryByCriteria(StudentEventTracking.class, criteria);
        query.addOrderByAscending(StudentEventTracking.COL_EVENT_DATE);

        m_studentEvents = getBroker().getMapByQuery(query, StudentEventTracking.COL_STUDENT_OID, 5000);
    }

    /**
     * converts numbers to English words to be displayed in the report
     * method added for portability purposes
     *
     * @param number
     * @return
     */
    private static String numbersToWords(int number) {
        String toWords;

        if (number % 100 < 20) {
            toWords = NUM_NAMES[number % 100];
            number /= 100;
        } else {
            toWords = NUM_NAMES[number % 10];
            number /= 10;

            toWords = TENS_NAMES[number % 10] + toWords;
            number /= 10;
        }

        if (number == 0) {
            return toWords;
        }

        return NUM_NAMES[number] + " hundred" + toWords;
    }

    /**
     * Adds the localization parameters Populates the Valid Locales map Initializes the
     */
    private void initializeLocalized() {
        Collection<OrganizationLocale> locales = getOrganization().getRootOrganization().getLocales();
        Map<String, MessageResources> resources = new HashMap<String, MessageResources>();
        // m_localized = !getBooleanParameter("englishOnly");
        m_validLocales = new HashMap<String, String>();
        m_defaultLocale = null;// start at null and check in case overwritten with invalid
        for (OrganizationLocale loc : locales) {
            if (loc.getEnabledIndicator()) {

                MessageResources messages =
                        LocalizationCache.getMessages(getBroker().getPersistenceKey(), loc.getLocale());
                // save the messages for that language
                resources.put(loc.getLocale(), messages);
                logToolMessage(Level.INFO, "adding " + loc.getName(), false);
                // populate the map of valid locales
                m_validLocales.put(loc.getName(), loc.getLocale());
                if (loc.getPrimaryIndicator())

                {
                    logToolMessage(Level.INFO, "making " + loc.getName() + " default", false);
                    m_defaultLocale = loc.getLocale();
                }
            }
        }
        if (m_defaultLocale == null || m_defaultLocale.isEmpty()) {
            m_defaultLocale = AMERICAN_ENGLISH_LOCALE;
        }
        addParameter("prefix", "tools." + getJob().getTool().getOid() + ".");
        addParameter(LOCALES, resources);
        addParameter(DEFAULT_LOCALE_PARAM, AMERICAN_ENGLISH_LOCALE);
    }

    /**
     * Builds criteria to the StudentAttendance.
     *
     * @param student
     * @param startDate
     * @param endDate
     *
     * @return Criteria
     */
    private Criteria buildAttendanceCriteria(SisStudent student, PlainDate startDate, PlainDate endDate) {
        Criteria criteria = new Criteria();
        criteria.addEqualTo(StudentAttendance.COL_STUDENT_OID, student.getOid());
        criteria.addGreaterOrEqualThan(StudentAttendance.COL_DATE, startDate);
        criteria.addLessOrEqualThan(StudentAttendance.COL_DATE, endDate);

        return criteria;
    }

    /**
     * Builds ColumnQuery to the Student Attendance
     * by the selected category column, series column and criteria
     *
     * @param categoryFieldOid
     * @param seriesFieldOid
     * @param criteria
     *
     * @return ColumnQuery
     */
    private ColumnQuery buildAttendanceQuery(String categoryFieldOid, String seriesFieldOid, Criteria criteria) {
        String[] columns;
        SisDataFieldConfig categoryFieldConfig =
                (SisDataFieldConfig) getBroker().getBeanByOid(SisDataFieldConfig.class, categoryFieldOid);
        SisDataFieldConfig seriesFieldConfig =
                (SisDataFieldConfig) getBroker().getBeanByOid(SisDataFieldConfig.class, seriesFieldOid);


        if (seriesFieldConfig != null) {
            columns = new String[2];
            columns[0] = categoryFieldConfig.getDataField().getJavaName();
            columns[1] = seriesFieldConfig.getDataField().getJavaName();
        } else {
            columns = new String[1];
            columns[0] = categoryFieldConfig.getDataField().getJavaName();
        }

        ColumnQuery query = new ColumnQuery(StudentAttendance.class, columns, criteria);

        return query;
    }

    /**
     * Populates chart grid.
     *
     * @param query
     *
     * @return ReportDataGrid
     */
    private ReportDataGrid populateChartGrid(ColumnQuery query) {
        ReportDataGrid grid = new ReportDataGrid();

        HashMap<String, BigDecimal> categoryMap = new HashMap<String, BigDecimal>();
        HashMap<String, String> seriesMap = new HashMap<String, String>();

        BigDecimal totalCount = new BigDecimal(0);

        ReportQueryIterator iterator = getBroker().getReportQueryIteratorByQuery(query);
        try {
            while (iterator.hasNext()) {
                Object[] row = (Object[]) iterator.next();

                String convertedCategory = convertToString(row[0]);
                int count =
                        categoryMap.containsKey(convertedCategory) ? categoryMap.get(convertedCategory).intValue() : 0;
                categoryMap.put(convertedCategory, BigDecimal.valueOf(count + 1));

                if (row.length > 1) {
                    String convertedSeries = convertToString(row[1]);
                    seriesMap.put(convertedCategory, convertedSeries);
                }

                totalCount = totalCount.add(new BigDecimal(1));
            }

            for (Map.Entry<String, BigDecimal> category : categoryMap.entrySet()) {
                grid.append();
                grid.set(COL_COUNT, category.getValue());
                grid.set(COL_CATEGORY, category.getKey());
                if (!seriesMap.isEmpty()) {
                    grid.set(COL_SERIES, seriesMap.get(category.getKey()));
                }
                grid.set(COL_TOTAL_COUNT, totalCount);
            }
        } finally {
            iterator.close();
        }

        grid.beforeTop();

        return grid;
    }

    /**
     * Converts passed value to String.
     * Returns converted value or empty string if can't convert value.
     *
     * @param value
     *
     * @return String
     */
    private String convertToString(Object value) {
        String result = "";

        if (value != null) {
            if (value instanceof String) {
                result = (String) value;
            } else if (value instanceof PlainDate || value instanceof Date) {
                Converter dateConverter = ConverterFactory.getConverterForClass(Converter.DATE_CONVERTER, getLocale());
                result = dateConverter.javaToString(value);
            } else if (value instanceof BigDecimal) {
                Converter decimalConverter =
                        ConverterFactory.getConverterForClass(Converter.BIG_DECIMAL_CONVERTER, getLocale());
                result = decimalConverter.javaToString(value);
            }
        }

        return result;
    }
}
