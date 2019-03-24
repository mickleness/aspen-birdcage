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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.activation.DataSource;
import javax.imageio.ImageIO;

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
import com.follett.fsc.core.framework.persistence.X2Criteria;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.business.PreferenceManager;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.x2dev.reports.portable.AttendanceWarningLetterData;
import com.x2dev.sis.model.beans.SisSchool;
import com.x2dev.sis.model.beans.SisStudent;
import com.x2dev.sis.model.beans.StudentAttendance;
import com.x2dev.sis.model.beans.path.SisBeanPaths;
import com.x2dev.utils.DateUtils;
import com.x2dev.utils.KeyValuePair;
import com.x2dev.utils.types.PlainDate;

import net.sf.jasperreports.engine.JRDataSource;

public class AttendanceWarningLetter2 extends AttendanceWarningLetterData {
	
	static class AttendanceChartDataSource implements DataSource {
		LinkedHashMap<Month, KeyValuePair<Integer, Double>> comparisonMap;
		String firstName;

		public AttendanceChartDataSource(LinkedHashMap<Month, KeyValuePair<Integer, Double>> comparisonMap, String firstName) {
			this.comparisonMap = comparisonMap;
			this.firstName = firstName;
		}

		@Override
		public String getContentType() {
			return "image/png";
		}

		@Override
		public InputStream getInputStream() throws IOException {
			JFreeChart chart = createChart(comparisonMap, firstName);
			byte[] pngImage;
			try(ByteArrayOutputStream byteOut = new ByteArrayOutputStream()) {
				BufferedImage bi = chart.createBufferedImage(800, 600);
				ImageIO.write(bi, "png", byteOut);
				pngImage = byteOut.toByteArray();
				return new ByteArrayInputStream(pngImage);
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String getName() {
			return "Chart";
		}

		@Override
		public OutputStream getOutputStream() {
			return null;
		}

		private JFreeChart createChart(LinkedHashMap<Month, KeyValuePair<Integer, Double>> comparisonMap,String studentFirstName) {
	        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
	        for(Entry<Month, KeyValuePair<Integer, Double>> entry : comparisonMap.entrySet()) {
	        	dataset.addValue( entry.getValue().getKey().doubleValue(), studentFirstName, entry.getKey());
	        	dataset.addValue( entry.getValue().getValue().doubleValue(), "Peers", entry.getKey());
	        }
		    
	        JFreeChart chart = ChartFactory.createBarChart(
	            "",         // chart title
	            "",               // domain axis label
	            "Absences",                  // range axis label
	            dataset,                  // data
	            PlotOrientation.VERTICAL, // orientation
	            true,                     // include legend
	            false,                     // tooltips?
	            false                     // URLs?
	        );

	        // set the background color for the chart...
	        //chart.setBackgroundPaint(Color.white);

	        // get a reference to the plot for further customisation...
	        final CategoryPlot plot = chart.getCategoryPlot();
	        plot.setRangeGridlinePaint(Color.black);
	        plot.setBackgroundPaint(Color.white);
//	        plot.setDomainGridlinePaint(Color.white);
//	        plot.setRangeGridlinePaint(Color.white);

	        // set the range axis to display integers only...
	        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
	        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
	        
	        // set up gradient paints for series...
	        final BarRenderer renderer = (BarRenderer) plot.getRenderer();
	        
	        renderer.setBarPainter(new BarPainter() {

				@Override
				public void paintBar(Graphics2D g2, BarRenderer renderer, int row, int column, RectangularShape bar, RectangleEdge base) {
					g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
					g2.setPaint(renderer.getSeriesPaint(row));
					g2.fill(bar);
					g2.setStroke(new BasicStroke(1));
					g2.setColor(Color.black);
					g2.draw(bar);
				}

				@Override
				public void paintBarShadow(Graphics2D g2, BarRenderer renderer, int row, int column, RectangularShape bar, RectangleEdge base, boolean pegShadow) {
					// unimplemented
				}
	        	
	        });
	        renderer.setSeriesPaint(0, ImageUtils.createStripedPaint(1, 3, 1, true));
	        renderer.setSeriesPaint(1, ImageUtils.createDottedPaint(1, 5, 10));

	        final CategoryAxis domainAxis = plot.getDomainAxis();
	        domainAxis.setCategoryLabelPositions(
	            CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 6.0)
	        );
			chart.setBackgroundPaint(Color.white);
			chart.setTextAntiAlias(true);
			
	        return chart;
		}
	}

	private static final long serialVersionUID = 1L;
    public static final String COL_COMPARISON_CHART_PNG = "comparisonChartPng";

    enum Month {
    	January, February, March, April, May, June, July, August, September, October, November, December;

		public static Month getMonth(PlainDate date) {
			GregorianCalendar gc = new GregorianCalendar();
			gc.setTime(date);
			return Month.values()[gc.get(GregorianCalendar.MONTH)];
		}
    }
    
    String activeCode;
    PlainDate endDate;
    PlainDate startDate;
    boolean excludeExcused;
    boolean includeAll;

	@Override
	protected JRDataSource gatherData() {
        activeCode = PreferenceManager.getPreferenceValue(getOrganization(), STUDENT_ACTIVE_CODE);
        endDate = (PlainDate) getParameter(END_DATE_PARAM);
        startDate = (PlainDate) getParameter(START_DATE_PARAM);
        excludeExcused = ((Boolean) getParameter(EXCLUDE_EXCUSED_PARAM)).booleanValue();
        includeAll = ((Boolean) getParameter(INCLUDE_ALL_PARAM)).booleanValue();
        
		ReportDataGrid data = (ReportDataGrid) super.gatherData();
		for(Map<String, Object> map : data.getRows()) {
			SisStudent std = (SisStudent) map.get(COL_STUDENT);
			LinkedHashMap<Month, KeyValuePair<Integer, Double>> comparisonMap = getStudentComparisonMap(std);
			if(comparisonMap!=null) {
				map.put(COL_COMPARISON_CHART_PNG, new AttendanceChartDataSource(comparisonMap, std.getPerson().getFirstName()));
			}
		}
		return data;
	}

	private LinkedHashMap<Month, KeyValuePair<Integer, Double>> getStudentComparisonMap(SisStudent std) {
		SisSchool skl = std.getSchool();
		if(skl==null)
			return null;
		
		X2Criteria studentCountCriteria = new X2Criteria();
        studentCountCriteria.addEqualTo(SisStudent.COL_GRADE_LEVEL, std.getGradeLevel());
        studentCountCriteria.addEqualTo(SisStudent.COL_SCHOOL_OID, std.getSchoolOid());
        studentCountCriteria.addEqualTo(SisStudent.COL_ENROLLMENT_STATUS, activeCode);
        studentCountCriteria.addNotEqualTo(SisStudent.COL_OID, std.getOid());
        BeanQuery studentCountQuery = new BeanQuery(SisStudent.class, studentCountCriteria);
        double studentCount = getBroker().getCount(studentCountQuery);
        
		X2Criteria thisStudentCriteria = new X2Criteria();
		thisStudentCriteria.addEqualTo(StudentAttendance.COL_STUDENT_OID, std.getOid());

		X2Criteria peersCriteria = new X2Criteria();
		peersCriteria.addNotEqualTo(SisBeanPaths.STUDENT_ATTENDANCE.studentOid().toString(), std.getOid());
		peersCriteria.addEqualTo(SisBeanPaths.STUDENT_ATTENDANCE.student().gradeLevel().toString(), std.getGradeLevel());
		peersCriteria.addEqualTo(SisBeanPaths.STUDENT_ATTENDANCE.student().enrollmentStatus().toString(), activeCode);
		
		Map<Month, Integer> peerAbsencesMap = getStudentAttendancesByMonth(skl.getOid(), startDate, endDate, peersCriteria);
		Map<Month, Integer> studentAbsenceMap = getStudentAttendancesByMonth(skl.getOid(), startDate, endDate, thisStudentCriteria);
		
		LinkedHashMap<Month, KeyValuePair<Integer, Double>> comparisonMap = new LinkedHashMap<>();
		for(PlainDate date = startDate; date.compareTo(endDate)<0; date = DateUtils.add(date, 1)) {
			Month month = Month.getMonth(date);
			if(!comparisonMap.containsKey(month)) {
				Number peerAbsences = peerAbsencesMap.get(month);
				Double peerAbsenceAverage;
				if(peerAbsences==null) {
					peerAbsenceAverage = Double.valueOf(0);
				} else {
					peerAbsenceAverage = Double.valueOf(peerAbsences.doubleValue() / studentCount);
				}
				
				Number stdAbsences = studentAbsenceMap.get(month);
				if(stdAbsences==null)
					stdAbsences = Integer.valueOf(0);
				comparisonMap.put(month, new KeyValuePair<>(Integer.valueOf(stdAbsences.intValue()), peerAbsenceAverage));
			}
		}
        return comparisonMap;
	}

	protected Map<Month, Integer> getStudentAttendancesByMonth(String sklOid, PlainDate startDate, PlainDate endDate, X2Criteria criteria) {
		Map<Month, AtomicInteger> returnValue = new HashMap<>();
		
		criteria.addEqualTo(StudentAttendance.COL_SCHOOL_OID, sklOid);
		criteria.addGreaterOrEqualThan(StudentAttendance.COL_DATE, startDate);
		criteria.addLessOrEqualThan(StudentAttendance.COL_DATE, endDate);
		//criteria.addEqualTo(StudentAttendance.COL_ABSENT_INDICATOR, Boolean.TRUE);
		if(excludeExcused) {
			criteria.addEqualTo(StudentAttendance.COL_EXCUSED_INDICATOR, Boolean.FALSE);
		}
		
		BeanQuery q = new BeanQuery(StudentAttendance.class, criteria);
		try(QueryIterator iter = getBroker().getIteratorByQuery(q)) {
			while(iter.hasNext()) {
				StudentAttendance att = (StudentAttendance) iter.next();
				Month month = Month.getMonth(att.getDate());
				AtomicInteger sum = returnValue.get(month);
				if(sum==null) {
					sum = new AtomicInteger(0);
					returnValue.put(month, sum);
				}
				sum.incrementAndGet();
			}
		}
		
		return (Map) returnValue;
	}
}

class ImageUtils {
	static Map<Object, Paint> DOTTED_PAINTS = new HashMap<>();
	static Map<Object, Paint> STRIPED_PAINTS = new HashMap<>();

	public static Paint createDottedPaint(int magnificationFactor, int horizontalGap, int verticalGap) {
		Object key = Arrays.asList(magnificationFactor, horizontalGap, verticalGap);
		Paint paint = DOTTED_PAINTS.get(key);
		if(paint==null) {
			BufferedImage bi = new BufferedImage(horizontalGap*magnificationFactor, verticalGap*magnificationFactor, BufferedImage.TYPE_INT_ARGB);
			double r = 1;
			Ellipse2D.Double dot = new Ellipse2D.Double(-r,-r,2*r,2*r);
			Graphics2D g = bi.createGraphics();
			g.scale(magnificationFactor, magnificationFactor);
			g.setColor(Color.black);
			g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
			Point2D[] offsets = new Point2D.Double[] {new Point2D.Double(horizontalGap,0), new Point2D.Double(0,verticalGap), new Point2D.Double(horizontalGap,verticalGap), new Point2D.Double(0,0), new Point2D.Double(horizontalGap/2,verticalGap/2), };
			for(Point2D offset : offsets) {
				Graphics2D g2 = (Graphics2D) g.create();
				g2.translate(offset.getX(), offset.getY());
				g2.fill(dot);
			}
			
			g.dispose();
			paint = new TexturePaint(bi, new Rectangle(0,0,bi.getWidth()/magnificationFactor, bi.getHeight()/magnificationFactor));
			DOTTED_PAINTS.put(key, paint);
		}
		return paint;
	}

	public static Paint createStripedPaint(int magnificationFactor, int darkWeight, int lightWeight, boolean slantUp) {
		Object key = Arrays.asList(magnificationFactor, darkWeight, lightWeight, slantUp);
		Paint paint = STRIPED_PAINTS.get(key);
		if(paint==null) {
			BufferedImage bi = new BufferedImage(10*magnificationFactor, 10*magnificationFactor, BufferedImage.TYPE_INT_ARGB);
			Graphics2D g = bi.createGraphics();
			g.scale(magnificationFactor, magnificationFactor);
			Line2D l = slantUp ? new Line2D.Double( -10, 20, 20, -10) : new Line2D.Double(-10, -10, 20, 20);
			g.setColor(Color.black);
			g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
			g.setStroke(new BasicStroke((float)(5*1.41422*darkWeight/(darkWeight + lightWeight))));
			g.draw(l);
			g.translate(0, -10);
			g.draw(l);
			g.translate(0, 20);
			g.draw(l);
			g.dispose();
			paint = new TexturePaint(bi, new Rectangle(0,0,bi.getWidth()/magnificationFactor, bi.getHeight()/magnificationFactor));
			STRIPED_PAINTS.put(key, paint);
		}
		return paint;
	}
}
