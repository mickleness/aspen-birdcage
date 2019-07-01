package org.abc.tools.reports;

import static com.follett.fsc.core.k12.beans.SystemPreferenceDefinition.STUDENT_ACTIVE_CODE;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Paint;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.TexturePaint;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Line2D;
import java.awt.geom.Point2D;
import java.awt.geom.RectangularShape;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import net.sf.jasperreports.engine.JRDataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;
import org.apache.struts.util.MessageResources;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarPainter;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.RectangleEdge;

import com.follett.fsc.core.framework.persistence.BeanQuery;
import com.follett.fsc.core.framework.persistence.ColumnQuery;
import com.follett.fsc.core.framework.persistence.X2Criteria;
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
import com.x2dev.sis.model.beans.path.SisBeanPaths;
import com.x2dev.utils.DateUtils;
import com.x2dev.utils.LoggerUtils;
import com.x2dev.utils.converters.Converter;
import com.x2dev.utils.converters.ConverterFactory;
import com.x2dev.utils.types.PlainDate;

public class AttendanceWarningLetter2 extends AttendanceWarningLetterData {

	enum Month {
		January, February, March, April, May, June, July, August, September, October, November, December;

		public static Month getMonth(PlainDate date) {
			GregorianCalendar gc = new GregorianCalendar();
			gc.setTime(date);
			return Month.values()[gc.get(GregorianCalendar.MONTH)];
		}
	}

	static class AbsenceComparison {
		int studentAbsences = 0;
		double peerAbsences = 0;
	}

	private static final long serialVersionUID = 1L;
	public static final String COL_COMPARISON_MONTH_CHART_PNG = "comparisonMonthChart";
	public static final String COL_COMPARISON_TOTAL_CHART_PNG = "comparisonTotalChart";

	private static final String PARAM_MONTHLY_CHART = "printMonthlyChartBar";
	private static final String PARAM_TOTAL_CHART = "printTotalChartBar";

	String activeCode;
	PlainDate endDate;
	PlainDate startDate;
	boolean excludeExcused;

	@Override
	protected JRDataSource gatherData() {
		getParameters().put(PRINT_CHART_BAR_PARAM, Boolean.FALSE);

		activeCode = PreferenceManager.getPreferenceValue(getOrganization(),
				STUDENT_ACTIVE_CODE);
		endDate = (PlainDate) getParameter(END_DATE_PARAM);
		startDate = (PlainDate) getParameter(START_DATE_PARAM);
		excludeExcused = ((Boolean) getParameter(EXCLUDE_EXCUSED_PARAM))
				.booleanValue();

		ReportDataGrid data = (ReportDataGrid) super.gatherData();

		Boolean monthlyChart = (Boolean) getParameter(PARAM_MONTHLY_CHART);
		Boolean totalChart = (Boolean) getParameter(PARAM_TOTAL_CHART);
		if (Boolean.TRUE.equals(monthlyChart)
				|| Boolean.TRUE.equals(totalChart)) {
			Map<SisStudent, Map<String, Object>> stdToRowMap = new HashMap<>();
			Map<String, Map<String, Collection<SisStudent>>> sklToGradeLevelToStdMap = new HashMap<>();
			for (Map<String, Object> map : data.getRows()) {
				SisStudent std = (SisStudent) map.get(COL_STUDENT);
				stdToRowMap.put(std, map);

				String sklOid = std.getSchoolOid();
				String gradeLevel = std.getGradeLevel();
				if (!StringUtils.isEmpty(sklOid)
						&& !StringUtils.isEmpty(gradeLevel)) {
					Map<String, Collection<SisStudent>> gradeLevelMap = sklToGradeLevelToStdMap
							.get(sklOid);
					if (gradeLevelMap == null) {
						gradeLevelMap = new HashMap<>();
						sklToGradeLevelToStdMap.put(sklOid, gradeLevelMap);
					}
					Collection<SisStudent> students = gradeLevelMap
							.get(gradeLevel);
					if (students == null) {
						students = new HashSet<>();
						gradeLevelMap.put(gradeLevel, students);
					}
					students.add(std);
				}
			}

			for (Entry<String, Map<String, Collection<SisStudent>>> sklToGradeLevelToStdEntry : sklToGradeLevelToStdMap
					.entrySet()) {
				String sklOid = sklToGradeLevelToStdEntry.getKey();

				Map<String, Collection<SisStudent>> studentsByGradeLevel = sklToGradeLevelToStdEntry
						.getValue();
				for (Entry<String, Collection<SisStudent>> studentsByGradeLevelEntry : studentsByGradeLevel
						.entrySet()) {
					String gradeLevel = studentsByGradeLevelEntry.getKey();

					X2Criteria studentCountCriteria = new X2Criteria();
					studentCountCriteria.addEqualTo(SisStudent.COL_GRADE_LEVEL,
							gradeLevel);
					studentCountCriteria.addEqualTo(SisStudent.COL_SCHOOL_OID,
							sklOid);
					studentCountCriteria.addEqualTo(
							SisStudent.COL_ENROLLMENT_STATUS, activeCode);
					BeanQuery studentCountQuery = new BeanQuery(
							SisStudent.class, studentCountCriteria);
					int gradeLevelSize = getBroker()
							.getCount(studentCountQuery);

					X2Criteria attendanceCriteria = new X2Criteria();
					attendanceCriteria.addEqualTo(
							SisBeanPaths.STUDENT_ATTENDANCE.student()
									.gradeLevel().toString(), gradeLevel);
					attendanceCriteria.addEqualTo(
							SisBeanPaths.STUDENT_ATTENDANCE.student()
									.schoolOid().toString(), sklOid);
					attendanceCriteria.addEqualTo(
							SisBeanPaths.STUDENT_ATTENDANCE.student()
									.enrollmentStatus().toString(), activeCode);
					attendanceCriteria.addGreaterOrEqualThan(
							SisBeanPaths.STUDENT_ATTENDANCE.date().toString(),
							startDate);
					attendanceCriteria.addLessOrEqualThan(
							SisBeanPaths.STUDENT_ATTENDANCE.date().toString(),
							endDate);
					if (excludeExcused) {
						attendanceCriteria.addEqualTo(
								SisBeanPaths.STUDENT_ATTENDANCE
										.excusedIndicator().toString(),
								Boolean.FALSE);
					}

					BeanQuery attendanceQuery = new BeanQuery(
							StudentAttendance.class, attendanceCriteria);
					Map<PlainDate, Map<String, StudentAttendance>> atts = getBroker()
							.getNestedMapByQuery(attendanceQuery,
									StudentAttendance.COL_DATE,
									StudentAttendance.COL_OID, 10, 10);

					for (SisStudent std : studentsByGradeLevelEntry.getValue()) {
						int totalAbsences = 0;
						int totalStudentAbsences = 0;
						LinkedHashMap<Month, AbsenceComparison> absencesByMonth = new LinkedHashMap<>();
						for (PlainDate date = startDate; date
								.compareTo(endDate) <= 0; date = DateUtils.add(
								date, 1)) {
							Month month = Month.getMonth(date);
							AbsenceComparison absenceInfo = absencesByMonth
									.get(month);
							if (absenceInfo == null) {
								absenceInfo = new AbsenceComparison();
								absencesByMonth.put(month, absenceInfo);
							}

							Map<String, StudentAttendance> attsForDate = atts
									.get(date);
							if (attsForDate != null) {
								for (StudentAttendance att : attsForDate
										.values()) {
									if (std.getOid()
											.equals(att.getStudentOid())) {
										absenceInfo.studentAbsences++;
										totalStudentAbsences++;
									}
									totalAbsences++;
									absenceInfo.peerAbsences += 1.0 / ((double) gradeLevelSize);
								}
							}
						}

						AbsenceComparison totalComparison = new AbsenceComparison();
						totalComparison.studentAbsences = totalStudentAbsences;
						totalComparison.peerAbsences = ((double) totalAbsences)
								/ ((double) (gradeLevelSize));

						JFreeChart chartByMonth = createMonthChart(
								absencesByMonth, std.getPerson().getFirstName());
						JFreeChart chartTotal = createTotalChart(
								totalComparison, std.getPerson().getFirstName());

						if (Boolean.TRUE.equals(monthlyChart))
							stdToRowMap.get(std).put(
									COL_COMPARISON_MONTH_CHART_PNG,
									createRenderer(chartByMonth));
						if (Boolean.TRUE.equals(totalChart))
							stdToRowMap.get(std).put(
									COL_COMPARISON_TOTAL_CHART_PNG,
									createRenderer(chartTotal));
					}
				}
			}
		}

		return data;
	}

	/**
	 * Wrap a chart in a JFreeChartRenderer.
	 * <p>
	 * This method only exists because the XR jar does not include the
	 * net.sf.jasperreports5.renderers.JFreeChartRenderer class.
	 * 
	 * @param chart
	 * @return
	 */
	private Object createRenderer(JFreeChart chart) {
		try {
			Class rendererClass = Class
					.forName("net.sf.jasperreports5.renderers.JFreeChartRenderer");
			Object renderer = rendererClass.getConstructor(JFreeChart.class)
					.newInstance(chart);
			return renderer;
		} catch (Exception e) {
			logToolMessage(Level.WARNING,
					LoggerUtils.convertThrowableToString(e), false);
			return null;
		}
	}

	private JFreeChart createMonthChart(
			LinkedHashMap<Month, AbsenceComparison> comparisonMap,
			String studentFirstName) {
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		for (Entry<Month, AbsenceComparison> entry : comparisonMap.entrySet()) {
			dataset.addValue(entry.getValue().studentAbsences,
					studentFirstName, entry.getKey());
			dataset.addValue(entry.getValue().peerAbsences, "Peers",
					entry.getKey());
		}
		return createChart(dataset);
	}

	private JFreeChart createTotalChart(AbsenceComparison comparison,
			String studentFirstName) {
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		dataset.addValue(comparison.studentAbsences, studentFirstName, "");
		dataset.addValue(comparison.peerAbsences, "Peers", "");
		return createChart(dataset);
	}

	private JFreeChart createChart(DefaultCategoryDataset dataset) {
		JFreeChart chart = ChartFactory.createBarChart("", // chart title
				"", // domain axis label
				"Absences", // range axis label
				dataset, // data
				PlotOrientation.VERTICAL, // orientation
				true, // include legend
				false, // tooltips?
				false // URLs?
				);

		final CategoryPlot plot = chart.getCategoryPlot();
		plot.setRangeGridlinePaint(Color.black);
		plot.setBackgroundPaint(Color.white);

		final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());

		final BarRenderer renderer = (BarRenderer) plot.getRenderer();
		renderer.setSeriesPaint(0,
				ImageUtils.createStripedPaint(10, 3, 1, true));
		renderer.setSeriesPaint(1, ImageUtils.createDottedPaint(10, 5, 10));
		renderer.setSeriesOutlinePaint(0, Color.black);
		renderer.setSeriesOutlinePaint(1, Color.black);
		renderer.setDrawBarOutline(true);
		renderer.setShadowVisible(false);

		// not sure why this is necessary, but some customers reported seeing
		// default blue bars instead of our custom B&W patterns
		renderer.setBarPainter(new BarPainter() {

			@Override
			public void paintBar(Graphics2D g2, BarRenderer renderer, int row,
					int column, RectangularShape bar, RectangleEdge base) {
				g2.setPaint(renderer.getSeriesPaint(row));
				g2.fill(bar);
				g2.setPaint(renderer.getSeriesOutlinePaint(row));
				g2.draw(bar);
			}

			@Override
			public void paintBarShadow(Graphics2D g2, BarRenderer renderer,
					int row, int column, RectangularShape bar,
					RectangleEdge base, boolean pegShadow) {
				// no shadow for us
			}

		});

		final CategoryAxis domainAxis = plot.getDomainAxis();
		domainAxis.setCategoryLabelPositions(CategoryLabelPositions
				.createUpRotationLabelPositions(Math.PI / 6.0));
		chart.setBackgroundPaint(Color.white);
		chart.setTextAntiAlias(true);

		return chart;
	}
}

class ImageUtils {
	static Map<Object, Paint> DOTTED_PAINTS = new HashMap<>();
	static Map<Object, Paint> STRIPED_PAINTS = new HashMap<>();

	public static Paint createDottedPaint(int magnificationFactor,
			int horizontalGap, int verticalGap) {
		Object key = Arrays.asList(magnificationFactor, horizontalGap,
				verticalGap);
		Paint paint = DOTTED_PAINTS.get(key);
		if (paint == null) {
			BufferedImage bi = new BufferedImage(horizontalGap
					* magnificationFactor, verticalGap * magnificationFactor,
					BufferedImage.TYPE_INT_ARGB);
			double r = 1;
			Ellipse2D.Double dot = new Ellipse2D.Double(-r, -r, 2 * r, 2 * r);
			Graphics2D g = bi.createGraphics();
			g.scale(magnificationFactor, magnificationFactor);
			g.setColor(Color.black);
			g.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
					RenderingHints.VALUE_ANTIALIAS_ON);
			Point2D[] offsets = new Point2D.Double[] {
					new Point2D.Double(horizontalGap, 0),
					new Point2D.Double(0, verticalGap),
					new Point2D.Double(horizontalGap, verticalGap),
					new Point2D.Double(0, 0),
					new Point2D.Double(horizontalGap / 2, verticalGap / 2), };
			for (Point2D offset : offsets) {
				Graphics2D g2 = (Graphics2D) g.create();
				g2.translate(offset.getX(), offset.getY());
				g2.fill(dot);
			}

			g.dispose();
			paint = new TexturePaint(bi,
					new Rectangle(0, 0, bi.getWidth() / magnificationFactor, bi
							.getHeight() / magnificationFactor));
			DOTTED_PAINTS.put(key, paint);
		}
		return paint;
	}

	public static Paint createStripedPaint(int magnificationFactor,
			int darkWeight, int lightWeight, boolean slantUp) {
		Object key = Arrays.asList(magnificationFactor, darkWeight,
				lightWeight, slantUp);
		Paint paint = STRIPED_PAINTS.get(key);
		if (paint == null) {
			BufferedImage bi = new BufferedImage(10 * magnificationFactor,
					10 * magnificationFactor, BufferedImage.TYPE_INT_ARGB);
			Graphics2D g = bi.createGraphics();
			g.scale(magnificationFactor, magnificationFactor);
			Line2D l = slantUp ? new Line2D.Double(-10, 20, 20, -10)
					: new Line2D.Double(-10, -10, 20, 20);
			g.setColor(Color.black);
			g.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
					RenderingHints.VALUE_ANTIALIAS_ON);
			g.setStroke(new BasicStroke(
					(float) (5 * 1.41422 * darkWeight / (darkWeight + lightWeight))));
			g.draw(l);
			g.translate(0, -10);
			g.draw(l);
			g.translate(0, 20);
			g.draw(l);
			g.dispose();
			paint = new TexturePaint(bi,
					new Rectangle(0, 0, bi.getWidth() / magnificationFactor, bi
							.getHeight() / magnificationFactor));
			STRIPED_PAINTS.put(key, paint);
		}
		return paint;
	}
}

/**
 * This is copied and pasted from the portable AttendanceWarningLetterData
 * class. It's branched here primarily to avoid the risk if Follett updates that
 * class and then someone imports the revised class above: they shouldn't lose
 * Follett's updated code.
 * <p>
 * Prepares the data for PORTABLE REPORTS PROJECT "Attendance Warning Letter"
 * report. This report lists the student attendance matching the total criteria
 * within the entered date range. It also creates StudentEventTracking records
 * for the students receiving letters.
 * <p>
 * This report takes the following consideration in the SQL based on the wording
 * of the letter are:
 * <ul>
 * <li>SQL will always query for students who have greater than the
 * parameterized absences (irrespective of Student Event records)
 * </ul>
 *
 * @author Follett School Solutions
 */
class AttendanceWarningLetterData extends ReportJavaSourceNet {
	/**
	 * Name for the "end date" report parameter. The value is a PlainDate.
	 */
	public static final String END_DATE_PARAM = "endDate";

	/**
	 * Name for the "exclude excused absences" report parameter. The value is a
	 * Boolean.
	 */
	public static final String EXCLUDE_EXCUSED_PARAM = "excludeExcused";

	/**
	 * Name for the "include students with previous mailing" report parameter.
	 * The value is a Boolean.
	 */
	public static final String INCLUDE_ALL_PARAM = "includeAll";

	/**
	 * Name for the "include students with previous mailing" report parameter.
	 * The value is a Boolean.
	 */
	public static final String GENERATE_EVENTS_PARAM = "generateEvents";

	/**
	 * Name for the "Minimum absences" report parameter. The value is an
	 * integer.
	 */
	public static final String MINIMUM_ABSENCES_PARAM = "minimumAbsences";

	/**
	 * Name for the "multiple mailings" report parameter. The value is a
	 * Boolean.
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
	 * Name for the "numbersToWords" parameter for portability. Value is a
	 * String.
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

	// private static final String EVENT_PREFIX =
	// "Attendance Failure Warning Letter - 4 Days";

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
	 * arrays used to convert numbers into English word for future portability
	 * reasons
	 */
	private static final String[] TENS_NAMES = { "", " ten", " twenty",
			" thirty", " forty", " fifty", " sixty", " seventy", " eighty",
			" ninety" };

	private static final String[] NUM_NAMES = { "", " one", " two", " three",
			" four", " five", " six", " seven", " eight", " nine", " ten",
			" eleven", " twelve", " thirteen", " fourteen", " fifteen",
			" sixteen", " seventeen", " eighteen", " nineteen" };

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
		 * Initializing localized parameters and values to allow for custom
		 * report sections
		 */
		initializeLocalized();

		/*
		 * Prepare the SQL parameters based on preferences/user input.
		 */
		String activeCode = PreferenceManager.getPreferenceValue(
				getOrganization(), STUDENT_ACTIVE_CODE);
		int minimumAbsences = ((Integer) getParameter(MINIMUM_ABSENCES_PARAM))
				.intValue();
		BigDecimal lowerBound = new BigDecimal(minimumAbsences);
		PlainDate endDate = (PlainDate) getParameter(END_DATE_PARAM);
		boolean excludeExcused = ((Boolean) getParameter(EXCLUDE_EXCUSED_PARAM))
				.booleanValue();
		boolean includeAll = ((Boolean) getParameter(INCLUDE_ALL_PARAM))
				.booleanValue();
		boolean generateEvents = ((Boolean) getParameter(GENERATE_EVENTS_PARAM))
				.booleanValue();
		PlainDate startDate = (PlainDate) getParameter(START_DATE_PARAM);
		boolean multipleMailings = ((Boolean) getParameter(MULTIPLE_MAILINGS_PARAM))
				.booleanValue();
		String eventComment = getParameter(EVENT_COMMENT_PARAM).toString();
		m_eventName = getParameter(EVENT_NAME_PARAM).toString();

		boolean printChartBar = ((Boolean) getParameter(PRINT_CHART_BAR_PARAM))
				.booleanValue();
		String categoryFieldOid = (String) getParameter(CHART_BAR_CATEGORIES_PARAM);
		String seriesFieldOid = (String) getParameter(CHART_BAR_SERIES_PARAM);

		String eventType = m_eventName; // + lowerBound;

		loadStudentEvents();

		/*
		 * Execute a SQL statement to get the students and their absence total.
		 * The SQL is easier to write/maintain even though a query is then run
		 * for every student that matches the result set.
		 */
		if (startDate != null && endDate != null && endDate.after(startDate)) {
			StringBuilder sql = new StringBuilder(512);

			sql.append("SELECT A.ATT_STD_OID, SUM(A.ATT_PORTION_ABSENT), T0.TRK_OID, STD_NAME_VIEW, STD_YOG, STD_HOMEROOM");
			sql.append("  FROM (");

			sql.append("   SELECT ATT_STD_OID, ATT_PORTION_ABSENT");
			sql.append("     FROM STUDENT_ATTENDANCE");
			sql.append("    INNER JOIN STUDENT ON ATT_STD_OID = STD_OID");
			sql.append("    WHERE ATT_DATE >= ?");
			sql.append("      AND ATT_DATE <= ?");
			sql.append("      AND STD_ENROLLMENT_STATUS = '" + activeCode + "'");
			sql.append("      AND STD_SKL_OID = '"
					+ ((SisSchool) getSchool()).getOid() + "'");

			if (m_studentOid != null) {
				sql.append("      AND STD_OID = '" + m_studentOid + "'");
			}

			if (excludeExcused) {
				sql.append("      AND ATT_EXCUSED_IND = '0'");
			}

			sql.append(") A INNER JOIN STUDENT ON A.ATT_STD_OID = STD_OID ");
			sql.append("    LEFT OUTER JOIN STUDENT_EVENT_TRACKING T0 ON T0.TRK_STD_OID = STD_OID ");
			sql.append("                                             AND T0.TRK_CTX_OID = '"
					+ ((SisOrganization) getOrganization())
							.getCurrentContextOid() + "'");
			sql.append("                                             AND T0.TRK_EVENT_TYPE = '"
					+ eventType + " >" + lowerBound.doubleValue() + " days'");
			sql.append(" GROUP BY A.ATT_STD_OID, T0.TRK_OID, STD_NAME_VIEW, STD_YOG, STD_HOMEROOM ");
			sql.append("HAVING SUM(A.ATT_PORTION_ABSENT) > "
					+ lowerBound.doubleValue());
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

			addParameter(NUMBERS_TO_WORDS_PARAM,
					numbersToWords(minimumAbsences));

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

					SisStudent student = (SisStudent) getBroker().getBeanByOid(
							Student.class, studentOid);

					StudentEventTracking event = (StudentEventTracking) m_studentEvents
							.get(studentOid);

					if (event == null || includeAll) {
						/*
						 * Add the student bean the absence total to a grid.
						 * Create a event record if one doesn't already exist.
						 */
						grid.append();
						grid.set(COL_STUDENT, student);
						grid.set(COL_ABSENT_TOTAL, new Double(absentTotal));
						grid.set(COL_ADDRESS, student.getPerson()
								.getResolvedMailingAddress());
						grid.set(COL_PRINT_CHART,
								Boolean.valueOf(printChartBar));

						if (printChartBar) {
							SisDataFieldConfig categoryFieldConfig = (SisDataFieldConfig) getBroker()
									.getBeanByOid(SisDataFieldConfig.class,
											categoryFieldOid);
							SisDataFieldConfig seriesFieldConfig = (SisDataFieldConfig) getBroker()
									.getBeanByOid(SisDataFieldConfig.class,
											seriesFieldOid);

							addParameter(CATEGORY,
									categoryFieldConfig.getUserLongName());

							if (seriesFieldConfig != null) {
								addParameter(SERIES,
										seriesFieldConfig.getUserLongName());
							}

							Criteria attendanceCriteria = buildAttendanceCriteria(
									student, startDate, endDate);
							ColumnQuery chartQuery = buildAttendanceQuery(
									categoryFieldOid, seriesFieldOid,
									attendanceCriteria);
							chartGrid = populateChartGrid(chartQuery);

							Report chartFormat = ReportUtils.getReport(
									SUB_CHART_REPORT_ID, getBroker());

							grid.set(
									COL_CHART_FORMAT,
									new ByteArrayInputStream(chartFormat
											.getCompiledFormat()));
							grid.set(COL_CHART_DATA, chartGrid);

							summaryGrid = populateChartGrid(chartQuery);

							Report summaryFormat = ReportUtils.getReport(
									SUB_CHART_SUMMARY_REPORT_ID, getBroker());

							grid.set(
									COL_SUMMARY_FORMAT,
									new ByteArrayInputStream(summaryFormat
											.getCompiledFormat()));
							grid.set(COL_SUMMARY_DATA, summaryGrid);
						}

						if (multipleMailings) {
							Collection contacts = (Collection) m_studentContacts
									.get(studentOid);
							if (contacts != null) {
								Iterator contactIterator = contacts.iterator();
								while (contactIterator.hasNext()) {
									StudentContact contact = (StudentContact) contactIterator
											.next();

									grid.append();
									grid.set(COL_STUDENT, student);
									grid.set(COL_ABSENT_TOTAL, new Double(
											absentTotal));
									grid.set(COL_ADDRESS, contact.getContact()
											.getPerson()
											.getResolvedMailingAddress());
									grid.set(COL_PRINT_CHART,
											Boolean.valueOf(printChartBar));

									if (printChartBar) {
										Report chartFormat = ReportUtils
												.getReport(SUB_CHART_REPORT_ID,
														getBroker());

										grid.set(
												COL_CHART_FORMAT,
												new ByteArrayInputStream(
														chartFormat
																.getCompiledFormat()));
										grid.set(COL_CHART_DATA, chartGrid);

										Report summaryFormat = ReportUtils
												.getReport(
														SUB_CHART_SUMMARY_REPORT_ID,
														getBroker());

										grid.set(
												COL_SUMMARY_FORMAT,
												new ByteArrayInputStream(
														summaryFormat
																.getCompiledFormat()));
										grid.set(COL_SUMMARY_DATA, summaryGrid);
									}
								}
							}
						}

						// Should not create an event if includeAll was checked
						// off.
						if (generateEvents) {
							if (event == null) {
								StudentEventTracking newEvent = X2BaseBean
										.newInstance(
												StudentEventTracking.class,
												getBroker().getPersistenceKey());

								newEvent.setDistrictContextOid(((SisOrganization) getOrganization())
										.getCurrentContextOid());
								newEvent.setEventDate(today);
								newEvent.setEventType(m_eventName + " >"
										+ minimumAbsences + " days");
								newEvent.setStudentOid(studentOid);
								newEvent.setComment(eventComment
										+ " --> Report run for date range: "
										+ startDate + " to " + endDate);

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
	 * Remember the currently selected student if this report is being run from
	 * the student module.
	 *
	 * @see com.x2dev.sis.tools.ToolJavaSource#saveState(com.x2dev.sis.web.UserDataContainer)
	 */
	@Override
	protected void saveState(UserDataContainer userData) {
		ContextList parentList = userData.getParentList();
		if (parentList != null
				&& parentList.getDataClass().equals(SisStudent.class)) {
			m_studentOid = parentList.getCurrentRecord().getOid();
		}
	}

	/**
	 * Loads the mailing contacts for students into a Map of StudentContacts
	 * keyed to student OID.
	 */
	private void loadMailingContacts() {
		Criteria criteria = new Criteria();
		criteria.addEqualTo(StudentContact.COL_CONDUCT_MAILING_INDICATOR,
				Boolean.TRUE);
		criteria.addNotEqualTo(StudentContact.COL_LIVES_WITH_INDICATOR,
				Boolean.TRUE);

		if (isSchoolContext()) {
			criteria.addEqualTo(StudentContact.REL_STUDENT + "."
					+ SisStudent.COL_SCHOOL_OID,
					((SisSchool) getSchool()).getOid());
		}

		QueryByCriteria query = new QueryByCriteria(StudentContact.class,
				criteria);
		m_studentContacts = getBroker().getGroupedCollectionByQuery(query,
				StudentContact.COL_STUDENT_OID, 2000);
	}

	/**
	 * Loads the student event records into a Map keyed to student OID.
	 */
	private void loadStudentEvents() {
		Criteria criteria = new Criteria();
		criteria.addEqualTo(StudentEventTracking.COL_DISTRICT_CONTEXT_OID,
				((SisOrganization) getOrganization()).getCurrentContextOid());
		criteria.addLike(StudentEventTracking.COL_EVENT_TYPE, m_eventName + "%");

		QueryByCriteria query = new QueryByCriteria(StudentEventTracking.class,
				criteria);
		query.addOrderByAscending(StudentEventTracking.COL_EVENT_DATE);

		m_studentEvents = getBroker().getMapByQuery(query,
				StudentEventTracking.COL_STUDENT_OID, 5000);
	}

	/**
	 * converts numbers to English words to be displayed in the report method
	 * added for portability purposes
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
	 * Adds the localization parameters Populates the Valid Locales map
	 * Initializes the
	 */
	private void initializeLocalized() {
		Collection<OrganizationLocale> locales = getOrganization()
				.getRootOrganization().getLocales();
		Map<String, MessageResources> resources = new HashMap<String, MessageResources>();
		// m_localized = !getBooleanParameter("englishOnly");
		m_validLocales = new HashMap<String, String>();
		m_defaultLocale = null;// start at null and check in case overwritten
								// with invalid
		for (OrganizationLocale loc : locales) {
			if (loc.getEnabledIndicator()) {

				MessageResources messages = LocalizationCache.getMessages(
						getBroker().getPersistenceKey(), loc.getLocale());
				// save the messages for that language
				resources.put(loc.getLocale(), messages);
				logToolMessage(Level.INFO, "adding " + loc.getName(), false);
				// populate the map of valid locales
				m_validLocales.put(loc.getName(), loc.getLocale());
				if (loc.getPrimaryIndicator())

				{
					logToolMessage(Level.INFO, "making " + loc.getName()
							+ " default", false);
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
	private Criteria buildAttendanceCriteria(SisStudent student,
			PlainDate startDate, PlainDate endDate) {
		Criteria criteria = new Criteria();
		criteria.addEqualTo(StudentAttendance.COL_STUDENT_OID, student.getOid());
		criteria.addGreaterOrEqualThan(StudentAttendance.COL_DATE, startDate);
		criteria.addLessOrEqualThan(StudentAttendance.COL_DATE, endDate);

		return criteria;
	}

	/**
	 * Builds ColumnQuery to the Student Attendance by the selected category
	 * column, series column and criteria
	 *
	 * @param categoryFieldOid
	 * @param seriesFieldOid
	 * @param criteria
	 *
	 * @return ColumnQuery
	 */
	private ColumnQuery buildAttendanceQuery(String categoryFieldOid,
			String seriesFieldOid, Criteria criteria) {
		String[] columns;
		SisDataFieldConfig categoryFieldConfig = (SisDataFieldConfig) getBroker()
				.getBeanByOid(SisDataFieldConfig.class, categoryFieldOid);
		SisDataFieldConfig seriesFieldConfig = (SisDataFieldConfig) getBroker()
				.getBeanByOid(SisDataFieldConfig.class, seriesFieldOid);

		if (seriesFieldConfig != null) {
			columns = new String[2];
			columns[0] = categoryFieldConfig.getDataField().getJavaName();
			columns[1] = seriesFieldConfig.getDataField().getJavaName();
		} else {
			columns = new String[1];
			columns[0] = categoryFieldConfig.getDataField().getJavaName();
		}

		ColumnQuery query = new ColumnQuery(StudentAttendance.class, columns,
				criteria);

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

		ReportQueryIterator iterator = getBroker()
				.getReportQueryIteratorByQuery(query);
		try {
			while (iterator.hasNext()) {
				Object[] row = (Object[]) iterator.next();

				String convertedCategory = convertToString(row[0]);
				int count = categoryMap.containsKey(convertedCategory) ? categoryMap
						.get(convertedCategory).intValue() : 0;
				categoryMap.put(convertedCategory,
						BigDecimal.valueOf(count + 1));

				if (row.length > 1) {
					String convertedSeries = convertToString(row[1]);
					seriesMap.put(convertedCategory, convertedSeries);
				}

				totalCount = totalCount.add(new BigDecimal(1));
			}

			for (Map.Entry<String, BigDecimal> category : categoryMap
					.entrySet()) {
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
	 * Converts passed value to String. Returns converted value or empty string
	 * if can't convert value.
	 *
	 * @param value
	 *
	 * @return String
	 */
	private String convertToString(Object value) {
		String result = "";

		String converterName = null;

		if (value != null) {
			if (value instanceof String) {
				result = (String) value;
			} else if (value instanceof PlainDate || value instanceof Date) {
				converterName = Converter.DATE_CONVERTER;
			} else if (value instanceof BigDecimal) {
				converterName = Converter.BIG_DECIMAL_CONVERTER;
			}

			if (converterName != null) {
				Converter converter = ConverterFactory.getConverterForClass(
						converterName, getLocale());
				result = converter.javaToString(value);
			}
		}

		return result;
	}
}
