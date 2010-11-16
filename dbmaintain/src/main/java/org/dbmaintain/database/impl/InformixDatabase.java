/*
 * Copyright DbMaintain.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbmaintain.database.impl;

import org.dbmaintain.database.Database;
import org.dbmaintain.database.DatabaseConnection;
import org.dbmaintain.database.DatabaseException;
import org.dbmaintain.database.IdentifierProcessor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;


/**
 * Implementation of {@link org.dbmaintain.database.Database} for a hsqldb database
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class InformixDatabase extends Database {

    public InformixDatabase(DatabaseConnection databaseConnection, IdentifierProcessor identifierProcessor) {
        super(databaseConnection, identifierProcessor);
    }

    @Override
    public String getSupportedDatabaseDialect() {
        return "informix";
    }

    @Override
    public Set<String> getTableNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select tabname from systables where owner = '" + schemaName + "' and tabid > 99 and tabtype = 'T'", getDataSource());
    }

    @Override
    public Set<String> getColumnNames(String schemaName, String tableName) {
        return getSQLHandler().getItemsAsStringSet("select sc.colname from syscolumns sc join systables st on sc.tabid = st.tabid and st.tabname = '" +
                tableName + "' and st.owner = '" + schemaName + "'", getDataSource());
    }

    @Override
    public Set<String> getViewNames(String schemaName) {
        return getSQLHandler().getItemsAsStringSet("select tabname from systables where owner = '" + schemaName + "' and tabid > 99 and tabtype = 'V'", getDataSource());
    }

    @Override
    public void disableReferentialConstraints(String schemaName) {
        disableConstraints(schemaName, "R");
    }

    @Override
    public void disableValueConstraints(String schemaName) {
        disableConstraints(schemaName, "N");
    }

    protected void disableConstraints(String schemaName, String constraintType) {
        Connection connection = null;
        Statement queryStatement = null;
        Statement alterStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getDataSource().getConnection();
            queryStatement = connection.createStatement();
            alterStatement = connection.createStatement();
            resultSet = queryStatement.executeQuery("SELECT SC.CONSTRNAME CONSTRAINTNAME FROM SYSCONSTRAINTS SC JOIN SYSTABLES ST " +
                    "ON SC.TABID = ST.TABID WHERE ST.OWNER='" + schemaName + "' AND SC.CONSTRTYPE='" + constraintType + "'");
            while (resultSet.next()) {
                String constraintName = resultSet.getString("CONSTRAINTNAME");
                alterStatement.executeUpdate("SET CONSTRAINTS " + quoted(constraintName) + " DISABLED");
            }
        } catch (SQLException e) {
            throw new DatabaseException("Unable to disable value constraints for schema name: " + schemaName, e);
        } finally {
            closeQuietly(queryStatement);
            closeQuietly(connection, alterStatement, resultSet);
        }
    }
}
