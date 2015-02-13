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
package org.dbmaintain.script.runner.impl;

import static java.lang.System.currentTimeMillis;
import static org.apache.commons.lang3.StringUtils.deleteWhitespace;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.util.FileUtils.createFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.database.Databases;
import org.dbmaintain.script.Script;
import org.dbmaintain.util.DbMaintainException;

/**
 * Implementation of a script runner that uses Oracle's SQL plus.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class SqlPlusScriptRunner extends BaseNativeScriptRunner {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(SqlPlusScriptRunner.class);

    protected Application application;
    protected String sqlPlusCommand;
    private Properties configuration;

    public SqlPlusScriptRunner(final Databases databases, final Properties configuration, final String sqlPlusCommand) {
        super(databases);
        this.configuration = configuration;
        this.sqlPlusCommand = sqlPlusCommand;
        application = createApplication(sqlPlusCommand);
    }

    @Override
    protected void executeScript(final File scriptFile, final Database targetDatabase) throws Exception {
        final File wrapperScriptFile = generateWrapperScriptFile(targetDatabase.getDatabaseInfo(), scriptFile);
        final String[] arguments = {"/nolog", "@" + wrapperScriptFile.getPath()};
        final Application.ProcessOutput processOutput = application.execute(arguments);
        final int exitValue = processOutput.getExitValue();
        logger.info("SQL*Plus exited with code:" + exitValue);
        if (exitValue != 0) {
            throw new DbMaintainException("Failed to execute command. SQL*Plus returned an error.\n" + processOutput.getOutput());
        }
    }

    protected File generateWrapperScriptFile(final DatabaseInfo databaseInfo, final File targetScriptFile) throws IOException {
        final File temporaryScriptsDir = createTemporaryScriptsDir();
        final File temporaryScriptWrapperFile = new File(temporaryScriptsDir, "wrapper-" + currentTimeMillis() + targetScriptFile.getName());
        temporaryScriptWrapperFile.deleteOnExit();

        final String lineSeparator = System.getProperty("line.separator");
        final StringBuilder content = new StringBuilder();
        // if property set use custom wrapper script
        if (PropertyUtils.getString(PROPERTY_SQL_PLUS_PRE_SCRIPT_FILE_PATH, getConfiguration()) != null) {
            // connect to DB
            content.append(lineSeparator);
            content.append("connect ");
            content.append(databaseInfo.getUserName());
            content.append('/');
            content.append(databaseInfo.getPassword());
            content.append('@');
            content.append(getDatabaseConfigFromJdbcUrl(databaseInfo.getUrl()));
            content.append(lineSeparator);
            content.append("alter session set current_schema=");
            content.append(databaseInfo.getDefaultSchemaName());
            content.append(";");
            content.append(lineSeparator);

            // read content from custom script file
            final String preScriptFilePath = PropertyUtils.getString(PROPERTY_SQL_PLUS_PRE_SCRIPT_FILE_PATH, getConfiguration());
            final String scriptEncoding = PropertyUtils.getString(PROPERTY_SCRIPT_ENCODING, getConfiguration());
            @SuppressWarnings("unchecked")
            final List<String> lines = FileUtils.readLines(new File(preScriptFilePath), scriptEncoding);
            for (final String line : lines) {
                content.append(line).append(lineSeparator);
            }
        } else {
            content.append("set echo off");
            content.append(lineSeparator);
            content.append("whenever sqlerror exit sql.sqlcode rollback");
            content.append(lineSeparator);
            content.append("whenever oserror exit sql.sqlcode rollback");
            content.append(lineSeparator);
            content.append("connect ");
            content.append(databaseInfo.getUserName());
            content.append('/');
            content.append(databaseInfo.getPassword());
            content.append('@');
            content.append(getDatabaseConfigFromJdbcUrl(databaseInfo.getUrl()));
            content.append(lineSeparator);
            content.append("alter session set current_schema=");
            content.append(databaseInfo.getDefaultSchemaName());
            content.append(";");
            content.append(lineSeparator);
            content.append("set echo on");
            content.append(lineSeparator);
        }
        content.append("@@");
        content.append(targetScriptFile.getName());
        content.append(lineSeparator);
        if (PropertyUtils.getString(PROPERTY_SQL_PLUS_POST_SCRIPT_FILE_PATH, getConfiguration()) != null) {
            // read content from custom script file
            final String postScriptFilePath = PropertyUtils.getString(PROPERTY_SQL_PLUS_POST_SCRIPT_FILE_PATH, getConfiguration());
            final String scriptEncoding = PropertyUtils.getString(PROPERTY_SCRIPT_ENCODING, getConfiguration());
            @SuppressWarnings("unchecked")
            final List<String> lines = FileUtils.readLines(new File(postScriptFilePath), scriptEncoding);
            for (final String line : lines) {
                content.append(line).append(lineSeparator);
            }
            content.append(lineSeparator);
        } else {
            content.append("exit sql.sqlcode");
            content.append(lineSeparator);
        }
        createFile(temporaryScriptWrapperFile, content.toString());
        return temporaryScriptWrapperFile;
    }

    /**
     * Oracle does not support blanks in file names, so remove them from the temp file name.
     *
     * @param script The script that is going to be executed, not null
     * @return The file name without spaces, not null
     */
    @Override
    protected String getTemporaryScriptName(final Script script) {
        final String temporaryScriptName = super.getTemporaryScriptName(script);
        return deleteWhitespace(temporaryScriptName);
    }

    protected Application createApplication(final String sqlPlusCommand) {
        return new Application("SQL*Plus", sqlPlusCommand);
    }

    protected String getDatabaseConfigFromJdbcUrl(final String url) {
        final int index = url.indexOf('@');
        if (index == -1) {
            return url;
        }
        return url.substring(index + 1);
    }

    private Properties getConfiguration() {
        return configuration;
    }
}
