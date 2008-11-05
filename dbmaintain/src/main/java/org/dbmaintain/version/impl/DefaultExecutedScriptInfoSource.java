/*
 * Copyright 2006-2007,  Unitils.org
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
package org.dbmaintain.version.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import static org.dbmaintain.thirdparty.org.apache.commons.dbutils.DbUtils.closeQuietly;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.version.ExecutedScriptInfoSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of <code>VersionSource</code> that stores the version in the database.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultExecutedScriptInfoSource implements ExecutedScriptInfoSource {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultExecutedScriptInfoSource.class);

    protected DbSupport defaultDbSupport;

    protected SQLHandler sqlHandler;

    protected Set<ExecutedScript> executedScripts;

    /**
     * The name of the database table in which the executed script info is stored
     */
    protected String executedScriptsTableName;

    /**
     * The name of the database column in which the script name is stored
     */
    protected String fileNameColumnName;
    protected int fileNameColumnSize;

    /**
     * The name of the database column in which the script version is stored
     */
    protected String versionColumnName;
    protected int versionColumnSize;

    /**
     * The name of the database column in which the file last modification timestamp is stored
     */
    protected String fileLastModifiedAtColumnName;

    /**
     * The name of the database column in which the checksum calculated on the script content is stored
     */
    protected String checksumColumnName;
    protected int checksumColumnSize;

    /**
     * The name of the database column in which the script execution timestamp is stored
     */
    protected String executedAtColumnName;
    protected int executedAtColumnSize;

    /**
     * The name of the database column in which the script name is stored
     */
    protected String succeededColumnName;

    /**
     * True if the scripts table should be created automatically if it does not exist yet
     */
    protected boolean autoCreateExecutedScriptsTable;

    /**
     * Format of the contents of the executed_at column
     */
    protected DateFormat timestampFormat;

    /**
     * The suffix for indicating that a script is a fix script, not null
     */
    protected String fixScriptSuffix;

    /**
     * The prefix to use for locating the target database part in the filename, not null
     */
    protected String targetDatabasePrefix;


    /**
     * Constructor for DefaultExecutedScriptInfoSource.
     * <p/>
     * todo javadoc
     *
     * @param autoCreateExecutedScriptsTable
     * @param executedScriptsTableName
     * @param fileNameColumnName
     * @param fileNameColumnSize
     * @param versionColumnName
     * @param versionColumnSize
     * @param fileLastModifiedAtColumnName
     * @param checksumColumnName
     * @param checksumColumnSize
     * @param executedAtColumnName
     * @param executedAtColumnSize
     * @param succeededColumnName
     * @param timestampFormat
     * @param defaultSupport
     * @param sqlHandler
     * @param fixScriptSuffix                The suffix for indicating that a script is a fix script, not null
     * @param targetDatabasePrefix           The prefix to use for locating the target database part in the filename, not null
     */
    public DefaultExecutedScriptInfoSource(boolean autoCreateExecutedScriptsTable, String executedScriptsTableName, String fileNameColumnName,
                                           int fileNameColumnSize, String versionColumnName, int versionColumnSize, String fileLastModifiedAtColumnName,
                                           String checksumColumnName, int checksumColumnSize, String executedAtColumnName, int executedAtColumnSize,
                                           String succeededColumnName, DateFormat timestampFormat, DbSupport defaultSupport, SQLHandler sqlHandler,
                                           String fixScriptSuffix, String targetDatabasePrefix) {

        this.defaultDbSupport = defaultSupport;
        this.sqlHandler = sqlHandler;
        this.autoCreateExecutedScriptsTable = autoCreateExecutedScriptsTable;
        this.executedScriptsTableName = defaultDbSupport.toCorrectCaseIdentifier(executedScriptsTableName);
        this.fileNameColumnName = defaultDbSupport.toCorrectCaseIdentifier(fileNameColumnName);
        this.fileNameColumnSize = fileNameColumnSize;
        this.versionColumnName = defaultDbSupport.toCorrectCaseIdentifier(versionColumnName);
        this.versionColumnSize = versionColumnSize;
        this.fileLastModifiedAtColumnName = defaultDbSupport.toCorrectCaseIdentifier(fileLastModifiedAtColumnName);
        this.checksumColumnName = defaultDbSupport.toCorrectCaseIdentifier(checksumColumnName);
        this.checksumColumnSize = checksumColumnSize;
        this.executedAtColumnName = defaultDbSupport.toCorrectCaseIdentifier(executedAtColumnName);
        this.executedAtColumnSize = executedAtColumnSize;
        this.succeededColumnName = defaultDbSupport.toCorrectCaseIdentifier(succeededColumnName);
        this.timestampFormat = timestampFormat;
        this.fixScriptSuffix = fixScriptSuffix;
        this.targetDatabasePrefix = targetDatabasePrefix;
    }
    
    
    /**
     * This method returns whether a from scratch update is recommended: It will return true
     * if the database is in it's initial state (i.e. the dbmaintain_scripts table doesn't exist yet 
     * or is invalid) and the autoCreateExecutedScriptsTable property is set to true.
     * <p/>
     * The reasoning behind this is that before executing the first script, it's a good idea to 
     * clear the database in order to start with a clean situation.
     * 
     * @return True if a from-scratch update is recommended
     */
    public boolean isFromScratchUpdateRecommended() {
        return !isExecutedScriptsTableValid() && autoCreateExecutedScriptsTable;
    }


    /**
     * @return All scripts that were registered as executed on the database
     */
    public Set<ExecutedScript> getExecutedScripts() {
        try {
            return doGetExecutedScripts();

        } catch (DbMaintainException e) {
            if (checkVersionTable()) {
                throw e;
            }
            // try again, executed scripts table was not ok
            return doGetExecutedScripts();
        }

    }


    /**
     * Precondition: The table db_executed_scripts must exist
     *
     * @return All scripts that were registered as executed on the database
     */
    protected Set<ExecutedScript> doGetExecutedScripts() {
        if (executedScripts != null) {
            return executedScripts;
        }

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = defaultDbSupport.getDataSource().getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery("select " + fileNameColumnName + ", " + versionColumnName + ", " + fileLastModifiedAtColumnName + ", " +
                    checksumColumnName + ", " + executedAtColumnName + ", " + succeededColumnName +
                    " from " + defaultDbSupport.qualified(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName));

            executedScripts = new HashSet<ExecutedScript>();
            while (resultSet.next()) {
                String fileName = resultSet.getString(fileNameColumnName);
                String checkSum = resultSet.getString(checksumColumnName);
                Long fileLastModifiedAt = resultSet.getLong(fileLastModifiedAtColumnName);
                Date executedAt = null;
                try {
                    executedAt = timestampFormat.parse(resultSet.getString(executedAtColumnName));
                } catch (ParseException e) {
                    throw new DbMaintainException("Error when parsing date " + executedAt + " using format " + timestampFormat, e);
                }
                boolean succeeded = resultSet.getInt(succeededColumnName) == 1;

                ExecutedScript executedScript = new ExecutedScript(new Script(fileName, fileLastModifiedAt, checkSum, fixScriptSuffix, targetDatabasePrefix), executedAt, succeeded);
                executedScripts.add(executedScript);
            }

        } catch (SQLException e) {
            throw new DbMaintainException("Error while retrieving database version", e);
        } finally {
            closeQuietly(connection, statement, resultSet);
        }
        return executedScripts;
    }


    /**
     * Registers the fact that the given script has been executed on the database
     *
     * @param executedScript The script that was executed on the database
     */
    public void registerExecutedScript(ExecutedScript executedScript) {
        try {
            doRegisterExecutedScript(executedScript);

        } catch (DbMaintainException e) {
            if (checkVersionTable()) {
                throw e;
            }
            // try again, version table was not ok
            doRegisterExecutedScript(executedScript);
        }
    }


    /**
     * Registers the fact that the given script has been executed on the database
     * Precondition: The table db_executed_scripts must exist
     *
     * @param executedScript The script that was executed on the database
     */
    protected void doRegisterExecutedScript(ExecutedScript executedScript) {
        if (getExecutedScripts().contains(executedScript)) {
            doUpdateExecutedScript(executedScript);
        } else {
            doSaveExecutedScript(executedScript);
        }
    }


    /**
     * Updates the given registered script
     *
     * @param executedScript
     */
    public void updateExecutedScript(ExecutedScript executedScript) {
        try {
            doUpdateExecutedScript(executedScript);

        } catch (DbMaintainException e) {
            if (checkVersionTable()) {
                throw e;
            }
            // try again, version table was not ok
            doUpdateExecutedScript(executedScript);
        }

    }


    /**
     * Saves the given registered script
     * Precondition: The table db_executed_scripts must exist
     *
     * @param executedScript
     */
    protected void doSaveExecutedScript(ExecutedScript executedScript) {
        executedScripts.add(executedScript);

        String executedAt = timestampFormat.format(executedScript.getExecutedAt());
        String insertSql = "insert into " + defaultDbSupport.qualified(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName) +
                " (" + fileNameColumnName + ", " + versionColumnName + ", " + fileLastModifiedAtColumnName + ", " + checksumColumnName + ", " +
                executedAtColumnName + ", " + succeededColumnName + ") values ('" + executedScript.getScript().getFileName() +
                "', '" + executedScript.getScript().getVersion().getIndexesString() + "', " + executedScript.getScript().getFileLastModifiedAt() + ", '" +
                executedScript.getScript().getCheckSum() + "', '" + executedAt + "', " + (executedScript.isSucceeded() ? "1" : "0") + ")";
        sqlHandler.executeUpdateAndCommit(insertSql, defaultDbSupport.getDataSource());
    }


    /**
     * Updates the given registered script
     * Precondition: The table db_executed_scripts must exist
     *
     * @param executedScript
     */
    protected void doUpdateExecutedScript(ExecutedScript executedScript) {
        executedScripts.add(executedScript);

        String executedAt = timestampFormat.format(executedScript.getExecutedAt());
        String updateSql = "update " + defaultDbSupport.qualified(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName) +
                " set " + checksumColumnName + " = '" + executedScript.getScript().getCheckSum() + "', " +
                fileLastModifiedAtColumnName + " = " + executedScript.getScript().getFileLastModifiedAt() + ", " +
                executedAtColumnName + " = '" + executedAt + "', " +
                succeededColumnName + " = " + (executedScript.isSucceeded() ? "1" : "0") +
                " where " + fileNameColumnName + " = '" + executedScript.getScript().getFileName() + "'";
        sqlHandler.executeUpdateAndCommit(updateSql, defaultDbSupport.getDataSource());
    }


    /**
     * Clears all script executions that have been registered. After having invoked this method,
     * {@link #getExecutedScripts()} will return an empty set.
     */
    public void clearAllExecutedScripts() {
        try {
            doClearAllExecutedScripts();

        } catch (DbMaintainException e) {
            if (checkVersionTable()) {
                throw e;
            }
            // try again, version table was not ok
            doClearAllExecutedScripts();
        }

    }


    /**
     * Actual implementation of the {@link #clearAllExecutedScripts()} method.
     */
    protected void doClearAllExecutedScripts() {
        executedScripts = new HashSet<ExecutedScript>();

        String deleteSql = "delete from " + defaultDbSupport.qualified(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName);
        sqlHandler.executeUpdateAndCommit(deleteSql, defaultDbSupport.getDataSource());
    }


    /**
     * Checks if the version table and columns are available and if a record exists in which the version info is stored.
     * If not, the table, columns and record are created if auto-create is true, else an exception is raised.
     *
     * @return False if the version table was not ok and therefore auto-created
     */
    protected boolean checkVersionTable() {
        // check valid
        if (isExecutedScriptsTableValid()) {
            return true;
        }

        // does not exist yet, if auto-create create version table
        if (autoCreateExecutedScriptsTable) {
            logger.warn("Executed scripts table " + defaultDbSupport.qualified(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName) + " doesn't exist yet or is invalid. A new one is created automatically.");
            createVersionTable();
            return false;
        }

        // throw an exception that shows how to create the version table
        String message = "Executed scripts table " + defaultDbSupport.qualified(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName) + " doesn't exist yet or is invalid.\n";
        message += "Please create it manually or let Unitils create it automatically by setting the dbMaintainer.autoCreateDbMaintainScriptsTable property to true.\n";
        message += "The table can be created manually by executing following statement:\n";
        message += getCreateVersionTableStatement();
        throw new DbMaintainException(message);
    }


    /**
     * Checks if the version table and columns are available and if a record exists in which the version info is stored.
     * If not, the table, columns and record are created.
     *
     * @return False if the version table was not ok and therefore re-created
     */
    protected boolean isExecutedScriptsTableValid() {
        // Check existence of version table
        Set<String> tableNames = defaultDbSupport.getTableNames(defaultDbSupport.getDefaultSchemaName());
        if (tableNames.contains(executedScriptsTableName)) {
            // Check columns of version table
            Set<String> columnNames = defaultDbSupport.getColumnNames(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName);
            if (columnNames.contains(fileNameColumnName) && columnNames.contains(versionColumnName) &&
                    columnNames.contains(fileLastModifiedAtColumnName) && columnNames.contains(checksumColumnName)
                    && columnNames.contains(executedAtColumnName) && columnNames.contains(succeededColumnName)) {
                return true;
            }
        }

        return false;
    }


    /**
     * Creates the version table and inserts a version record.
     */
    protected void createVersionTable() {
        // If version table is invalid, drop and re-create
        try {
            defaultDbSupport.dropTable(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName);
        } catch (DbMaintainException e) {
            // ignored
        }

        // Create db version table
        sqlHandler.executeUpdateAndCommit(getCreateVersionTableStatement(), defaultDbSupport.getDataSource());
    }


    /**
     * @return The statement to create the version table.
     */
    protected String getCreateVersionTableStatement() {
        String longDataType = defaultDbSupport.getLongDataType();
        return "create table " + defaultDbSupport.qualified(defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName) + " ( " +
                fileNameColumnName + " " + defaultDbSupport.getTextDataType(fileNameColumnSize) + ", " +
                versionColumnName + " " + defaultDbSupport.getTextDataType(versionColumnSize) + ", " +
                fileLastModifiedAtColumnName + " " + defaultDbSupport.getLongDataType() + ", " +
                checksumColumnName + " " + defaultDbSupport.getTextDataType(checksumColumnSize) + ", " +
                executedAtColumnName + " " + defaultDbSupport.getTextDataType(executedAtColumnSize) + ", " +
                succeededColumnName + " " + longDataType + " )";
    }


}
