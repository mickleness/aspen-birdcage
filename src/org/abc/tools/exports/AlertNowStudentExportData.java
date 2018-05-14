/* ++++ Upgraded by PhoenixUpgradeProcedure ++++ on Wed Feb 22 18:09:23 CST 2012 *//*
 * ====================================================================
 *
 * X2 Development Corporation
 *
 * Copyright (c) 2002-2011 X2 Development Corporation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, is not permitted without express written agreement
 * from X2 Development Corporation.
 *
 * ====================================================================
 */

package org.abc.tools.exports;

import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;
import static com.follett.fsc.core.k12.beans.SystemPreferenceDefinition.STUDENT_ACTIVE_CODE;

import java.util.*;

import org.abc.tools.Tool;
import org.apache.ojb.broker.query.*;

import com.follett.cust.cub.ExportJavaSourceCub;
import com.follett.fsc.core.k12.tools.exports.ExportJavaSource;
import com.x2dev.sis.model.beans.*;
import com.follett.fsc.core.k12.beans.*;
import com.follett.fsc.core.k12.business.*;
import com.follett.fsc.core.framework.persistence.*;
import com.x2dev.utils.*;

/**
 * Data source for Alert Now Student Export for Melrose
 * 
 * @author X2 Development Corporation
 */
public class AlertNowStudentExportData extends ExportJavaSourceCub
{
    /**
     * Name for the "active only" report parameter. The value is a Boolean.
     */
    public static final String ACTIVE_ONLY_PARAM = "activeOnly";
    
    /**
     * Name for the enumerated "query by" report parameter. The value is an Integer.
     */
    public static final String QUERY_BY_PARAM = "queryBy";
    
    /**
     * Name for the "query string" report parameter. The value is a String.
     */
    public static final String QUERY_STRING_PARAM = "queryString";
    
    // Grid constants
    private static final String FIELD_LOCAL_ID		                = "STUDENTID";
    private static final String FIELD_FIRST_NAME			= "FIRSTNAME";
    private static final String FIELD_LAST_NAME		        = "LASTNAME";
    private static final String FIELD_GENDER              		= "GENDER";
    private static final String FIELD_INSTITUTION              	= "SCHOOLNAME";
    private static final String FIELD_GRADE_LEVEL			= "YOG";
    private static final String FIELD_PRIMARY_PHONE_1	= "PrimaryPhone";
    private static final String FIELD_PRIMARY_PHONE_2      	= "Additional Phone";
    private static final String FIELD_PRIMARY_EMAIL_01	= "EmailAddress";
    private static final String FIELD_PRIMARY_EMAIL_02	= "EmailAddressAlt";
    private static final String FIELD_GRP_1				= "GROUP1";
 
    private Map<String, StudentContact>					m_emergencyPriority1Contact;
    private Map<String, StudentContact>					m_emergencyPriority2Contact;
  

    /**
     * @see com.x2dev.sis.tools.exports.ExportJavaSource#gatherData()
     */
    @Override
    protected DataGrid gatherData() throws Exception
    {
        DataGrid grid = new DataGrid();

        X2Criteria criteria  = new X2Criteria();
        
        String queryBy = (String) getParameter(QUERY_BY_PARAM);
        String queryString = (String) getParameter(QUERY_STRING_PARAM);
        
        addUserCriteria(criteria, queryBy, queryString, null, null);

        boolean activeOnly = ((Boolean) getParameter(ACTIVE_ONLY_PARAM)).booleanValue();
        if (activeOnly)
        {
            String activeCode = PreferenceManager.getPreferenceValue(getOrganization(), STUDENT_ACTIVE_CODE);
            criteria.addEqualTo(SisStudent.COL_ENROLLMENT_STATUS, activeCode);
        }
        
        if (isSchoolContext())
        {
        	criteria.addEqualTo(SisStudent.COL_SCHOOL_OID, ((SisSchool) getSchool()).getOid());
        }
        
        QueryByCriteria query = new QueryByCriteria(SisStudent.class, criteria);
        
        if (!isSchoolContext())
        {
            query.addOrderByAscending(SisStudent.REL_SCHOOL + PATH_DELIMITER + SisSchool.COL_SCHOOL_ID);
        }
        
        query.addOrderByAscending(SisStudent.COL_NAME_VIEW);
        
        int count = getBroker().getCount(query);
        loadContact(criteria, 1, count);
        loadContact(criteria, 2, count);
        loadContact(criteria, 3, count);
        loadContact(criteria, 4, count);
        
        QueryIterator iterator = getBroker().getIteratorByQuery(query);
        try
        {
            while (iterator.hasNext())
            {
                SisStudent student = (SisStudent) iterator.next();
                
                addStudentToGrid(grid, student);
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
     * Adds the student fields to the grid
     * 
     * @param grid
     * @param student
     */
    private void addStudentToGrid(DataGrid grid, SisStudent student)
    {                        
        grid.append();
        
        SisPerson person = student.getPerson();
        
       grid.set(FIELD_LOCAL_ID, student.getLocalId());        
       grid.set(FIELD_FIRST_NAME, person.getFirstName());
       grid.set(FIELD_LAST_NAME, person.getLastName());
       grid.set(FIELD_GENDER, person.getGenderCode());
       grid.set(FIELD_INSTITUTION, student.getSchool().getName());
       grid.set(FIELD_GRADE_LEVEL, String.valueOf(student.getYog()));
        
    }
    
    /**
     * Loads a map of student contact keyed on student Oid for the passed emergency priority
     * 
     * @param studentCriteria
     * @param emergencyPriority
     * @param mapSize
     */
    private void loadContact(Criteria studentCriteria, int emergencyPriority, int mapSize)
    {
    	Map<String, StudentContact> contactMap;
    	
    	Criteria criteria = new Criteria();
    	criteria.addEqualTo(StudentContact.COL_EMERGENCY_PRIORITY, Integer.valueOf(emergencyPriority));
    	
    	SubQuery subQuery = new SubQuery(SisStudent.class, X2BaseBean.COL_OID, studentCriteria);
    	criteria.addIn(StudentContact.COL_STUDENT_OID, subQuery);
    	
    	QueryByCriteria query = new QueryByCriteria(StudentContact.class, criteria);
    	
    	// If there are multiple contacts with same emergency priority, contact that was created first will be included 
    	query.addOrderByDescending(X2BaseBean.COL_OID);
    	
    	contactMap = getBroker().getMapByQuery(query, StudentContact.COL_STUDENT_OID, mapSize);
    	
    	if (emergencyPriority == 1)
    	{
    		m_emergencyPriority1Contact = contactMap;
    	}
    	else if (emergencyPriority == 2)
    	{
    		m_emergencyPriority2Contact = contactMap;
    	}
    	
    }
}