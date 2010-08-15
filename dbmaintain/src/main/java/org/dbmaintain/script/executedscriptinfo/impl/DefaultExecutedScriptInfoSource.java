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
package org.dbmaintain.script.executedscriptinfo.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.SQLHandler;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.util.DbMaintainException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;

import static org.apache.commons.dbutils.DbUtils.closeQuietly;

/**
 * Implementation of <code>VersionSource</code> that stores the version in the database.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultExecutedScriptInfoSource implements ExecutedScriptInfoSource {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultExecutedScriptInfoSource.class);

    protected Database defaultDatabase;
    protected SQLHandler sqlHandler;
    protected SortedSet<ExecutedScript> executedScripts;
    /* The name of the database table in which the executed script info is stored */
    protected String executedScriptsTableName;
    /* The name of the database column in which the script name is stored */
    protected String fileNameColumnName;
    protected int fileNameColumnSize;
    /* The name of the database column in which the file last modification timestamp is stored */
    protected String fileLastModifiedAtColumnName;
    /* The name of the database column in which the checksum calculated on the script content is stored */
    protected String checksumColumnName;
    protected int checksumColumnSize;
    /* The name of the database column in which the script execution timestamp is stored */
    protected String executedAtColumnName;
    protected int executedAtColumnSize;
    /* The name of the database column in which the script name is stored */
    protected String succeededColumnName;
    /* True if the scripts table should be created automatically if it does not exist yet */
    protected boolean autoCreateExecutedScriptsTable;
    /* Format of the contents of the executed_at column */
    protected DateFormat timestampFormat;
    /* The prefix to use for locating the target database part in the filename, not null */
    protected String targetDatabasePrefix;
    /* The prefix that can be used in the filename to identify qualifiers */
    protected String qualifierPefix;
    /* The registered (allowed) custom qualifiers */
    protected Set<Qualifier> registeredQualifiers;
    /* The qualifiers that identify a script as a patch script, not null */
    protected Set<Qualifier> patchQualifiers;
    /* The name of the post processing dir*/
    protected String postProcessingScriptDirName;
    /* The baseline revision. If set, all scripts with a lower revision will be ignored */
    protected ScriptIndexes baseLineRevision;


    public DefaultExecutedScriptInfoSource(boolean autoCreateExecutedScriptsTable, String executedScriptsTableName, String fileNameColumnName,
                                           int fileNameColumnSize, String fileLastModifiedAtColumnName, String checksumColumnName, int checksumColumnSize,
                                           String executedAtColumnName, int executedAtColumnSize, String succeededColumnName, DateFormat timestampFormat,
                                           Database defaultSupport, SQLHandler sqlHandler, String targetDatabasePrefix, String qualifierPrefix,
                                           Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String postProcessingScriptDirName,
                                           ScriptIndexes baseLineRevision) {

        this.defaultDatabase = defaultSupport;
        this.sqlHandler = sqlHandler;
        this.autoCreateExecutedScriptsTable = autoCreateExecutedScriptsTable;
        this.executedScriptsTableName = defaultDatabase.toCorrectCaseIdentifier(executedScriptsTableName);
        this.fileNameColumnName = defaultDatabase.toCorrectCaseIdentifier(fileNameColumnName);
        this.fileNameColumnSize = fileNameColumnSize;
        this.fileLastModifiedAtColumnName = defaultDatabase.toCorrectCaseIdentifier(fileLastModifiedAtColumnName);
        this.checksumColumnName = defaultDatabase.toCorrectCaseIdentifier(checksumColumnName);
        this.checksumColumnSize = checksumColumnSize;
        this.executedAtColumnName = defaultDatabase.toCorrectCaseIdentifier(executedAtColumnName);
        this.executedAtColumnSize = executedAtColumnSize;
        this.succeededColumnName = defaultDatabase.toCorrectCaseIdentifier(succeededColumnName);
        this.timestampFormat = timestampFormat;
        this.targetDatabasePrefix = targetDatabasePrefix;
        this.qualifierPefix = qualifierPrefix;
        this.registeredQualifiers = registeredQualifiers;
        this.patchQualifiers = patchQualifiers;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.baseLineRevision = baseLineRevision;
    }


    /**
     * @return All scripts that were registered as executed on the database
     */
    public SortedSet<ExecutedScript> getExecutedScripts() {
        try {
            return doGetExecutedScripts();

        } catch (DbMaintainException e) {
            if (checkExecutedScriptsTable()) {
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
    protected SortedSet<ExecutedScript> doGetExecutedScripts() {
        if (executedScripts != null) {
            return executedScripts;
        }

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = defaultDatabase.getDataSource().getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery("select " + fileNameColumnName + ", " + fileLastModifiedAtColumnName + ", " +
                    checksumColumnName + ", " + executedAtColumnName + ", " + succeededColumnName +
                    " from " + getQualifiedExecutedScriptsTableName());

            executedScripts = new TreeSet<ExecutedScript>();
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

                Script script = new Script(fileName, fileLastModifiedAt, checkSum, targetDatabasePrefix, qualifierPefix, registeredQualifiers, patchQualifiers, postProcessingScriptDirName, baseLineRevision);
                if (!script.isIgnored()) {
                    ExecutedScript executedScript = new ExecutedScript(script, executedAt, succeeded);
                    executedScripts.add(executedScript);
                }
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
        if (getExecutedScripts().contains(executedScript)) {
            updateExecutedScript(executedScript);
        } else {
            insertExecutedScript(executedScript);
        }
    }


    /**
     * Saves the given registered script
     * Precondition: The table db_executed_scripts must exist
     *
     * @param executedScript The script that needs to be saved, not null
     */
    protected void insertExecutedScript(ExecutedScript executedScript) {
        getExecutedScripts().add(executedScript);

        String executedAt = timestampFormat.format(executedScript.getExecutedAt());
        String insertSql = "insert into " + getQualifiedExecutedScriptsTableName() +
                " (" + fileNameColumnName + ", " + fileLastModifiedAtColumnName + ", " + checksumColumnName + ", " +
                executedAtColumnName + ", " + succeededColumnName + ") values ('" + executedScript.getScript().getFileName() +
                "', " + executedScript.getScript().getFileLastModifiedAt() + ", '" +
                executedScript.getScript().getCheckSum() + "', '" + executedAt + "', " + (executedScript.isSuccessful() ? "1" : "0") + ")";
        sqlHandler.executeUpdateAndCommit(insertSql, defaultDatabase.getDataSource());
    }


    /**
     * Updates the given registered script
     *
     * @param executedScript The script that needs to be updated, not null
     */
    public void updateExecutedScript(ExecutedScript executedScript) {
        getExecutedScripts().add(executedScript);

        String executedAt = timestampFormat.format(executedScript.getExecutedAt());
        String updateSql = "update " + getQualifiedExecutedScriptsTableName() +
                " set " + checksumColumnName + " = '" + executedScript.getScript().getCheckSum() + "', " +
                fileLastModifiedAtColumnName + " = " + executedScript.getScript().getFileLastModifiedAt() + ", " +
                executedAtColumnName + " = '" + executedAt + "', " +
                succeededColumnName + " = " + (executedScript.isSuccessful() ? "1" : "0") +
                " where " + fileNameColumnName + " = '" + executedScript.getScript().getFileName() + "'";
        sqlHandler.executeUpdateAndCommit(updateSql, defaultDatabase.getDataSource());
    }


    /**
     * Remove the given executed script from the executed scripts
     *
     * @param executedScript The executed script, which is no longer part of the executed scripts
     */
    public void deleteExecutedScript(ExecutedScript executedScript) {
        getExecutedScripts().remove(executedScript);

        String deleteSql = "delete from " + getQualifiedExecutedScriptsTableName() +
                " where " + fileNameColumnName + " = '" + executedScript.getScript().getFileName() + "'";
        sqlHandler.executeUpdateAndCommit(deleteSql, defaultDatabase.getDataSource());
    }


    /**
     * Registers the fact that the script that was originally executed has been renamed.
     *
     * @param executedScript  the original executed script that still refers to the original script
     * @param renamedToScript the script to which the original script has been renamed
     */
    public void renameExecutedScript(ExecutedScript executedScript, Script renamedToScript) {
        String renameSql = "update " + getQualifiedExecutedScriptsTableName() +
                " set " + fileNameColumnName + " = '" + renamedToScript.getFileName() + "', " +
                checksumColumnName + " = '" + renamedToScript.getCheckSum() + "', " +
                fileLastModifiedAtColumnName + " = " + renamedToScript.getFileLastModifiedAt() +
                " where " + fileNameColumnName + " = '" + executedScript.getScript().getFileName() + "'";
        sqlHandler.executeUpdateAndCommit(renameSql, defaultDatabase.getDataSource());
        executedScript.renameTo(renamedToScript);
    }


    public void deleteAllExecutedPostprocessingScripts() {
        for (Iterator<ExecutedScript> executedScriptsIterator = getExecutedScripts().iterator(); executedScriptsIterator.hasNext();) {
            ExecutedScript executedScript = executedScriptsIterator.next();
            if (executedScript.getScript().isPostProcessingScript()) {
                executedScriptsIterator.remove();
                String deleteSql = "delete from " + getQualifiedExecutedScriptsTableName() +
                        " where " + fileNameColumnName + " = '" + executedScript.getScript().getFileName() + "'";
                sqlHandler.executeUpdateAndCommit(deleteSql, defaultDatabase.getDataSource());
            }
        }
    }


    /**
     * Clears all script executions that have been registered. After having invoked this method,
     * {@link #getExecutedScripts()} will return an empty set.
     */
    public void clearAllExecutedScripts() {
        getExecutedScripts().clear();

        String deleteSql = "delete from " + getQualifiedExecutedScriptsTableName();
        sqlHandler.executeUpdateAndCommit(deleteSql, defaultDatabase.getDataSource());
    }


    /**
     * Marks the failed scripts in the executed scripts table as successful.
     */
    public void markErrorScriptsAsSuccessful() {
        String deleteSql = "update " + getQualifiedExecutedScriptsTableName() + " set " + succeededColumnName + "=1 where " + succeededColumnName + "=0";
        sqlHandler.executeUpdateAndCommit(deleteSql, defaultDatabase.getDataSource());
        // reset executed scripts
        executedScripts = null;
    }

    /**
     * Removes the failed scripts in the executed scripts table.
     */
    public void removeErrorScripts() {
        String deleteSql = "delete from " + getQualifiedExecutedScriptsTableName() + " where " + succeededColumnName + "=0";
        sqlHandler.executeUpdateAndCommit(deleteSql, defaultDatabase.getDataSource());
        // reset executed scripts
        executedScripts = null;
    }


    /**
     * Checks if the version table and columns are available and if a record exists in which the version info is stored.
     * If not, the table, columns and record are created if auto-create is true, else an exception is raised.
     *
     * @return false if the version table was not ok and therefore auto-created
     */
    protected boolean checkExecutedScriptsTable() {
        // check valid
        if (isExecutedScriptsTableValid()) {
            return true;
        }

        // does not exist yet, if auto-create create version table
        if (autoCreateExecutedScriptsTable) {
            logger.warn("Executed scripts table " + getQualifiedExecutedScriptsTableName() + " doesn't exist yet or is invalid. A new one is created automatically.");
            createExecutedScriptsTable();
            return false;
        }

        // throw an exception that shows how to create the version table
        String message = "Executed scripts table " + getQualifiedExecutedScriptsTableName() + " doesn't exist yet or is invalid.\n";
        message += "Please create it manually or let DbMaintain create it automatically by setting the property autoCreateDbMaintainScriptsTable to true.\n";
        message += "The table can be created manually by executing following statement:\n";
        message += getCreateExecutedScriptTableStatement();
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
        Set<String> tableNames = defaultDatabase.getTableNames(defaultDatabase.getDefaultSchemaName());
        if (tableNames.contains(executedScriptsTableName)) {
            // Check columns of version table
            Set<String> columnNames = defaultDatabase.getColumnNames(defaultDatabase.getDefaultSchemaName(), executedScriptsTableName);
            if (columnNames.contains(fileNameColumnName) && columnNames.contains(fileLastModifiedAtColumnName)
                    && columnNames.contains(checksumColumnName) && columnNames.contains(executedAtColumnName)
                    && columnNames.contains(succeededColumnName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Creates the version table and inserts a version record.
     */
    protected void createExecutedScriptsTable() {
        // If version table is invalid, drop and re-create
        try {
            defaultDatabase.dropTable(defaultDatabase.getDefaultSchemaName(), executedScriptsTableName);
        } catch (DbMaintainException e) {
            // ignored
        }

        // Create db version table
        sqlHandler.executeUpdateAndCommit(getCreateExecutedScriptTableStatement(), defaultDatabase.getDataSource());
    }

    /**
     * @return The statement to create the version table.
     */
    protected String getCreateExecutedScriptTableStatement() {
        String longDataType = defaultDatabase.getLongDataType();
        return "create table " + getQualifiedExecutedScriptsTableName() + " ( " +
                fileNameColumnName + " " + defaultDatabase.getTextDataType(fileNameColumnSize) + ", " +
                fileLastModifiedAtColumnName + " " + defaultDatabase.getLongDataType() + ", " +
                checksumColumnName + " " + defaultDatabase.getTextDataType(checksumColumnSize) + ", " +
                executedAtColumnName + " " + defaultDatabase.getTextDataType(executedAtColumnSize) + ", " +
                succeededColumnName + " " + longDataType + " )";
    }

    protected String getQualifiedExecutedScriptsTableName() {
        return defaultDatabase.qualified(defaultDatabase.getDefaultSchemaName(), executedScriptsTableName);
    }
}
