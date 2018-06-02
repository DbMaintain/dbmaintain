/*
 * Copyright 2010 DbMaintain.org
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.File;
import org.apache.commons.lang3.StringUtils;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.DatabaseInfo;
import org.dbmaintain.database.Databases;
import org.dbmaintain.util.DbMaintainException;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

/**
 * Implementation of a script runner that executes Shell Scripts.
 * 
 * @author Christian Liebhardt
 */
public class ShellScriptRunner extends BaseNativeScriptRunner {
    
    private static Log logger = LogFactory.getLog(ShellScriptRunner.class);

    private String chmodCommand;

    public ShellScriptRunner(Databases databases, String chmodCommand) {
        super(databases);
        this.chmodCommand = chmodCommand;
    }
    
    @Override
    protected void executeScript(File scriptFile, Database targetDatabase) {
        chmodScript(scriptFile);
        
        DatabaseInfo databaseInfo = targetDatabase.getDatabaseInfo();
        String[] arguments = {
                databaseInfo.getUserName(),
                databaseInfo.getPassword(),
                databaseInfo.getName(),
                databaseInfo.getUrl(),
                databaseInfo.getDefaultSchemaName(),
                databaseInfo.getDialect() 
            };
        Application application = createApplication(scriptFile.getAbsolutePath());
        Application.ProcessOutput processOutput = application.execute(arguments);
        int exitValue = processOutput.getExitValue();
        if (exitValue != 0) {
            throw new DbMaintainException("Failed to execute command "+scriptFile.getAbsolutePath()+" . Executable returned an error.\n" + processOutput.getOutput());
        }
        logger.info("Stdout of "+scriptFile.getAbsolutePath()+":\n"+processOutput.getOutput());
    }
    
    
    /**
     * Makes the given file executable
     * @param scriptFile File which shall be executable
     */
    private void chmodScript(File scriptFile) {
        if (chmodCommand.equals(""))
            return;
        String[] chmodCmdWithArgs = StringUtils.split(chmodCommand);
        List<String> chmodArgs = new ArrayList<>(Arrays.asList(chmodCmdWithArgs).subList(1, chmodCmdWithArgs.length));
        chmodArgs.add(scriptFile.getAbsolutePath());
        Application chmod = new Application("chmod command", chmodCmdWithArgs[0]);
        chmod.execute(chmodArgs.toArray(new String[0]));
    }

    protected Application createApplication(String command) {
        return new Application("Custom Executable", command);
    }
    
    protected String getDatabaseConfigFromJdbcUrl(String url) {
        int index = url.indexOf('@');
        if (index == -1) {
            return url;
        }
        return url.substring(index + 1);
    }
}
