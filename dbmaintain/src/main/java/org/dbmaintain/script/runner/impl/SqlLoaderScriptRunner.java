/*
 * Copyright DbMaintain.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * under the License.
 */

package org.dbmaintain.script.runner.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.dbmaintain.database.Database;
import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.database.Databases;
import org.dbmaintain.util.DbMaintainException;

/**
 * Implementation of a script runner that uses Oracle's SQL plus.
 * 
 * @author Christian Liebhardt
 */
public class SqlLoaderScriptRunner extends BaseNativeScriptRunner {

    protected Application application;
    protected String sqlLoaderCommand;
    
    public SqlLoaderScriptRunner(Databases databases, String sqlLoaderCommand) {
        super(databases);
        this.sqlLoaderCommand = sqlLoaderCommand;
        this.application = createApplication(sqlLoaderCommand);
    }
    
    @Override
    protected void executeScript(File scriptFile, Database targetDatabase)
            throws Exception {
        File tmpLog = null;
        File tmpBad = null;
        File tmpDiscard = null;
        try {
            DatabaseInfo databaseInfo = targetDatabase.getDatabaseInfo();
            tmpLog = File.createTempFile("sqlLdr",".log");
            tmpBad = File.createTempFile("sqlLdr",".bad");
            tmpDiscard = File.createTempFile("sqlLdr",".discard");
            String[] arguments = {databaseInfo.getUserName()+"/"+databaseInfo.getPassword()+"@"+getDatabaseConfigFromJdbcUrl(databaseInfo.getUrl()), 
                                  scriptFile.getAbsolutePath(),
                                  "errors=0",
                                  "discardmax=0",
                                  "log="+tmpLog.getAbsolutePath(),
                                  "bad="+tmpBad.getAbsolutePath(),
                                  "discard="+tmpDiscard.getAbsolutePath()};
            Application.ProcessOutput processOutput = application.execute(arguments);
            int exitValue = processOutput.getExitValue();
            if (exitValue != 0) {
                throw new DbMaintainException("Failed to execute command. SQL*Loader returned an error.\n" + arguments[0] + "\n" + 
                                               processOutput.getOutput()+"\n\n" +
                                               "Log file:\n" + 
                                               getFileContent(tmpLog) +
                                               "Bad file:\n" +
                                               getFileContent(tmpBad) +
                                               "Discard file:\n" +
                                               getFileContent(tmpDiscard));
            }
        } 
        finally {
            if (tmpLog != null)     tmpLog.delete();
            if (tmpBad != null)     tmpBad.delete();
            if (tmpDiscard != null) tmpDiscard.delete();
        }
    }
    
    private String getFileContent(File file) throws IOException {
        if (!file.exists()) {
            return file.getAbsolutePath() + " doesn't exist";
        }
        if (!file.canRead()) {
            return file.getAbsolutePath() + " can't be read";
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            StringBuilder builder = new StringBuilder();
            String line = "";
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append("\n");
            }
            return builder.toString();
        }
    }

    protected Application createApplication(String sqlLoaderCommand) {
        return new Application("SQL*Loader", sqlLoaderCommand);
    }
    
    protected String getDatabaseConfigFromJdbcUrl(String url) {
        final int index = url.indexOf('@');
        if (index == -1) {
            return url;
        }
        return url.substring(index + 1);
    }
}
