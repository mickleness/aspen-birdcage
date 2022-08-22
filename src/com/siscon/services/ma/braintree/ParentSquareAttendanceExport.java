/*
 * ====================================================================
 *
 * SIS Consulting Services
 *
 * Copyright (c) 2012 SIS Consulting Services.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, is not permitted without express written agreement
 * from SIS Consulting Services.
 *
 * ====================================================================
 */

package com.siscon.services.ma.braintree;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.ojb.broker.query.*;

import com.x2dev.sis.model.beans.*;
import com.x2dev.sis.model.beans.path.SisBeanPaths;
import com.follett.fsc.core.framework.persistence.X2Criteria;
import com.follett.fsc.core.k12.beans.*;
import com.follett.fsc.core.k12.tools.exports.ExportJavaSource;
import com.x2dev.utils.*;
import com.x2dev.utils.types.PlainDate;

/**
 * Data source for the "Parent Square - Attendance" export.
 *
 * @author SIS Consulting Services
 */
public class ParentSquareAttendanceExport extends ExportJavaSource
{
    // Input paramaters
    private static final String END_DATE_PARAM          = "endDate";
    private static final String START_DATE_PARAM        = "startDate";
   
    // Grid fields
    private static final String FIELD_STUDENT_ID        = "student_id";
    private static final String FIELD_DATE              = "date";
    private static final String FIELD_ATTENDANCE_CODE   = "attendance_code";
   
    // Private variables
    private List<String> m_columns;
   
    /**
     * @see com.x2dev.sis.tools.exports.ExportJavaSource#gatherData()
     */
    @Override
    protected DataGrid gatherData() throws Exception
    {
        DataGrid grid = new DataGrid(m_columns.size());
       
        SimpleDateFormat dateFormat = new SimpleDateFormat("M/d/yyyy");
       
        X2Criteria criteria = new X2Criteria();
        criteria.addGreaterOrEqualThan(StudentAttendance.COL_DATE, getParameter(START_DATE_PARAM));
        criteria.addLessOrEqualThan(StudentAttendance.COL_DATE, getParameter(END_DATE_PARAM));
        criteria.addNotEqualTo(SisBeanPaths.STUDENT_ATTENDANCE.field().A002(), "false");
       
        ReportQueryByCriteria query = new ReportQueryByCriteria(StudentAttendance.class,
                new String[] {
                        SisBeanPaths.STUDENT_ATTENDANCE.student().localId().toString(),
                        StudentAttendance.COL_DATE,
                        StudentAttendance.COL_CODE_VIEW
                },
                criteria);
       
        query.addOrderByAscending(StudentAttendance.COL_DATE);
        query.addOrderByAscending(SisBeanPaths.STUDENT_ATTENDANCE.student().localId().toString());
       
        ReportQueryIterator iterator = getBroker().getReportQueryIteratorByQuery(query);
        try
        {
            while (iterator.hasNext())
            {
                Object[] row = (Object[]) iterator.next();
               
                grid.append();                
                grid.set(FIELD_STUDENT_ID, row[0]);
                grid.set(FIELD_DATE, dateFormat.format(new PlainDate((Timestamp) row[1])));
                grid.set(FIELD_ATTENDANCE_CODE, row[2]);
               
            }
        }
        finally
        {
            iterator.close();
        }
       
        grid.beforeTop();

        return grid;
    }

    /**
     * @see com.x2dev.sis.tools.exports.ExportJavaSource#getColumnNames()
     */
    @Override
    protected List getColumnNames()
    {
        return m_columns;
    }

    /**
     * @see com.x2dev.sis.tools.exports.ExportJavaSource#getColumnUserNames()
     */
    @Override
    protected List getColumnUserNames()
    {
        return m_columns;
    }

    /**
     * @see com.x2dev.sis.tools.exports.ExportJavaSource#getComment()
     */
    @Override
    protected String getComment()
    {
        return null;
    }

    /**
     * @see com.x2dev.sis.tools.exports.ExportJavaSource#getHeader()
     */
    @Override
    protected String getHeader()
    {
        return null;
    }
   
    /**
     * @see com.x2dev.sis.tools.ToolJavaSource#initialize()
     */
    @Override
    protected void initialize()
    {
        m_columns = new ArrayList<String>(4);
        m_columns.add(FIELD_STUDENT_ID);
        m_columns.add(FIELD_DATE);
        m_columns.add(FIELD_ATTENDANCE_CODE);
    }
}