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
package org.dbmaintain.scriptrunner.impl.db2;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.scriptrunner.impl.Application;
import org.dbmaintain.scriptrunner.impl.BaseNativeScriptRunner;
import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.dbmaintain.scriptrunner.impl.db2.Db2ConnectionInfo.parseFromJdbcUrl;

/**
 * todo javadoc
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class Db2ScriptRunner extends BaseNativeScriptRunner {

    protected Application application;
    protected Map<DbSupport, Db2ConnectionInfo> db2ConnectionInfos;


    public Db2ScriptRunner(DbSupport defaultDbSupport, Map<String, DbSupport> nameDbSupportMap, String db2Command) {
        super(defaultDbSupport, nameDbSupportMap);
        this.application = createApplication(db2Command);
        this.db2ConnectionInfos = getDb2ConnectionInfos(nameDbSupportMap);
    }


    public void initialize() {
        for (Db2ConnectionInfo db2ConnectionInfo : db2ConnectionInfos.values()) {
            unregisterDatabaseQuietly(db2ConnectionInfo);
            registerDatabase(db2ConnectionInfo);
        }
    }

    public void close() {
        try {
            for (Db2ConnectionInfo db2ConnectionInfo : db2ConnectionInfos.values()) {
                unregisterDatabase(db2ConnectionInfo);
            }
        } finally {
            terminateDb2Connections();
        }
    }

    protected void terminateDb2Connections() {
        try {
            executeCommand("terminate");

        } catch (Exception e) {
            throw new DbMaintainException("Unable to terminate database connections.", e);
        }
    }

    protected void registerDatabase(Db2ConnectionInfo db2ConnectionInfo) {
        try {
            if (db2ConnectionInfo.isRemote()) {
                executeCommand("catalog tcpip node " + db2ConnectionInfo.getDatabaseAlias() + " remote " + db2ConnectionInfo.getHost() + " server " + db2ConnectionInfo.getPort());
                executeCommand("catalog database " + db2ConnectionInfo.getDatabaseName() + " as " + db2ConnectionInfo.getDatabaseAlias() + " at node " + db2ConnectionInfo.getDatabaseAlias());
            }
        } catch (Exception e) {
            throw new DbMaintainException("Unable to register database alias " + db2ConnectionInfo.getDatabaseAlias(), e);
        }
    }


    protected void unregisterDatabaseQuietly(Db2ConnectionInfo db2ConnectionInfo) {
        try {
            unregisterDatabase(db2ConnectionInfo);
        } catch (Exception e) {
            // ignored
        }
    }

    protected void unregisterDatabase(Db2ConnectionInfo db2ConnectionInfo) {
        try {
            if (db2ConnectionInfo.isRemote()) {
                executeCommand("uncatalog database " + db2ConnectionInfo.getDatabaseAlias());
                executeCommand("uncatalog node " + db2ConnectionInfo.getDatabaseAlias());
            }
        } catch (Exception e) {
            throw new DbMaintainException("Unable to unregister database alias " + db2ConnectionInfo.getDatabaseAlias(), e);
        }
    }

    @Override
    protected void executeScript(File scriptFile, DbSupport targetDbSupport) throws Exception {
        Db2ConnectionInfo db2ConnectionInfo = db2ConnectionInfos.get(targetDbSupport);

        executeCommand(false, "connect to " + db2ConnectionInfo.getDatabaseAlias() + " user " + db2ConnectionInfo.getUserName() + " using " + db2ConnectionInfo.getPassword());
        executeCommand("set SQLCOMPAT PLSQL");
        executeCommand("set schema " + targetDbSupport.getDefaultSchemaName());
        executeCommand("-t", "-s", "-v", "-c-", "-f" + scriptFile.getPath());
        executeCommand("commit");
    }

    protected void executeCommand(String... command) {
        executeCommand(true, command);
    }

    protected void executeCommand(boolean logCommand, String... command) {
        Application.ProcessOutput processOutput = application.execute(logCommand, command);
        int exitValue = processOutput.getExitValue();
        if (exitValue == 4 || exitValue == 8) {
            throw new DbMaintainException("Failed to execute command. DB2 CLP returned an error.\n" + processOutput.getOutput());
        }
    }

    protected Application createApplication(String db2Command) {
        Map<String, String> environmentVariables = new HashMap<String, String>();
        // workaround to be able to use db2 clp without db2cmd on windows
        environmentVariables.put("DB2CLP", "**$$**");
        return new Application("DB2 CLP", db2Command, environmentVariables);
    }


    protected Map<DbSupport, Db2ConnectionInfo> getDb2ConnectionInfos(Map<String, DbSupport> nameDbSupportMap) {
        Map<DbSupport, Db2ConnectionInfo> result = new HashMap<DbSupport, Db2ConnectionInfo>();

        int aliasCount = 1;
        for (DbSupport dbSupport : nameDbSupportMap.values()) {
            String remoteAlias = "dbm" + aliasCount++;
            DatabaseInfo databaseInfo = dbSupport.getDatabaseInfo();
            Db2ConnectionInfo db2ConnectionInfo = parseFromJdbcUrl(databaseInfo.getUrl(), remoteAlias, databaseInfo.getUserName(), databaseInfo.getPassword());
            result.put(dbSupport, db2ConnectionInfo);
        }
        return result;
    }

}