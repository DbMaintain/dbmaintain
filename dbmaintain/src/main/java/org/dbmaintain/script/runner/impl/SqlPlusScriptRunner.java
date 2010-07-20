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
package org.dbmaintain.script.runner.impl;

import org.dbmaintain.database.Database;
import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.database.Databases;
import org.dbmaintain.script.Script;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.io.IOException;

import static java.lang.System.currentTimeMillis;
import static org.apache.commons.lang.StringUtils.deleteWhitespace;
import static org.dbmaintain.util.FileUtils.createFile;

/**
 * Implementation of a script runner that uses Oracle's SQL plus.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class SqlPlusScriptRunner extends BaseNativeScriptRunner {

    protected Application application;
    protected String sqlPlusCommand;


    public SqlPlusScriptRunner(Databases databases, String sqlPlusCommand) {
        super(databases);
        this.sqlPlusCommand = sqlPlusCommand;
        this.application = createApplication(sqlPlusCommand);
    }


    @Override
    protected void executeScript(File scriptFile, Database targetDatabase) throws Exception {
        File wrapperScriptFile = generateWrapperScriptFile(targetDatabase.getDatabaseInfo(), scriptFile);
        String[] arguments = {"/nolog", "@" + wrapperScriptFile.getPath()};
        Application.ProcessOutput processOutput = application.execute(arguments);
        int exitValue = processOutput.getExitValue();
        if (exitValue != 0) {
            throw new DbMaintainException("Failed to execute command. SQL*Plus returned an error.\n" + processOutput.getOutput());
        }
    }


    protected File generateWrapperScriptFile(DatabaseInfo databaseInfo, File targetScriptFile) throws IOException {
        File temporaryScriptsDir = createTemporaryScriptsDir();
        File temporaryScriptWrapperFile = new File(temporaryScriptsDir, "wrapper-" + currentTimeMillis() + targetScriptFile.getName());
        temporaryScriptWrapperFile.deleteOnExit();

        String lineSeparator = System.getProperty("line.separator");
        StringBuilder content = new StringBuilder();
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
        content.append("@@");
        content.append(targetScriptFile.getName());
        content.append(lineSeparator);
        content.append("exit sql.sqlcode");
        content.append(lineSeparator);
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
    protected String getTemporaryScriptName(Script script) {
        String temporaryScriptName = super.getTemporaryScriptName(script);
        return deleteWhitespace(temporaryScriptName);
    }

    protected Application createApplication(String sqlPlusCommand) {
        return new Application("SQL*Plus", sqlPlusCommand);
    }

    protected String getDatabaseConfigFromJdbcUrl(String url) {
        int index = url.indexOf('@');
        if (index == -1) {
            return url;
        }
        return url.substring(index + 1);
    }
}
