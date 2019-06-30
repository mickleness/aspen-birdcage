package org.abc.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.follett.fsc.core.k12.business.X2Broker;
import com.x2dev.utils.ThreadUtils;

/**
 * This helper class handles the mechanics of setting up a SQL query. Subclasses
 * only have to implement the {@link #process(X2Broker, String, ResultSet)} method
 * to process each individual row of results.
 */
public abstract class SqlResultHandler {
	
	/**
	 * This processes one row of a SQL query.
	 * 
	 * @param broker the broker used for the current query.
	 * @param sql the SQL query that the ResultSet relates to.
	 * @param resultSet a result set from the SQL query.
	 * @return true if we should keep iterating over results, false otherwise.
	 */
	public abstract boolean process(X2Broker broker,String sql,ResultSet resultSet) throws SQLException;
	
	/**
	 * This executes a SQL query and calls {@link #process(X2Broker, String, ResultSet)} for every row.
	 * 
	 * @param broker the broker used to execute the query.
	 * @param sql the SQL to query.
	 * @return true if every ResultSet of the query was processed, false if this returned early.
	 * @throws SQLException
	 */
    public boolean executeSQL(X2Broker broker,String sql) throws SQLException {
    	Connection connection = broker.borrowConnection();
        Statement statement = null;
        ResultSet resultSet = null;

        try
        {
            statement = connection.createStatement();

            resultSet = statement.executeQuery(sql);
            while (resultSet.next())
            {
            	ThreadUtils.checkInterrupt();
            	if(!process(broker, sql, resultSet))
            		return false;
            }
        }
        finally
        {
            if (statement != null)
            {
                statement.close();
            }

            broker.returnConnection();
        }
        return true;
    }
}