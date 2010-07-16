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
package org.dbmaintain.util;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DbSupports;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.dbsupport.impl.HsqldbDbSupport;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.executedscriptinfo.impl.DefaultExecutedScriptInfoSource;
import org.dbmaintain.script.*;
import org.dbmaintain.script.impl.ArchiveScriptLocation;
import org.dbmaintain.script.impl.FileSystemScriptLocation;
import org.dbmaintain.script.impl.ScriptLocation;
import org.dbmaintain.script.impl.ScriptRepository;

import javax.sql.DataSource;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.dbmaintain.dbsupport.DbMaintainDataSource.createDataSource;
import static org.dbmaintain.util.CollectionUtils.asSet;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public abstract class TestUtils {


    public static DbSupports getDbSupports() {
        return getDbSupports("PUBLIC");
    }

    public static DatabaseInfo getHsqlDatabaseInfo(String... schemaNames) {
        if (schemaNames == null || schemaNames.length == 0) {
            schemaNames = new String[]{"PUBLIC"};
        }
        return new DatabaseInfo("mydatabase", "hsqldb", "org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:unitils", "sa", "", asList(schemaNames), false);
    }

    public static DbSupports getDbSupports(String... schemaNames) {
        DatabaseInfo databaseInfo = getHsqlDatabaseInfo(schemaNames);
        DataSource dataSource = createDataSource(databaseInfo);
        DbSupport dbSupport = new HsqldbDbSupport(databaseInfo, dataSource, new DefaultSQLHandler(), null, null);
        return new DbSupports(asList(dbSupport), new ArrayList<String>());
    }

    public static DefaultExecutedScriptInfoSource getDefaultExecutedScriptInfoSource(DbSupport dbSupport, boolean autoCreateExecutedScriptsTable) {
        return getDefaultExecutedScriptInfoSource(dbSupport, autoCreateExecutedScriptsTable, null);
    }

    public static DefaultExecutedScriptInfoSource getDefaultExecutedScriptInfoSource(DbSupport dbSupport, boolean autoCreateExecutedScriptsTable, ScriptIndexes baselineRevision) {
        return new DefaultExecutedScriptInfoSource(autoCreateExecutedScriptsTable,
                "dbmaintain_scripts", "file_name", 150, "file_last_modified_at", "checksum", 50, "executed_at", 50, "succeeded",
                new SimpleDateFormat("dd/MM/yyyy"), dbSupport, new DefaultSQLHandler(), "@", "#", Collections.<Qualifier>emptySet(),
                asSet(new Qualifier("patch")), "postprocessing", baselineRevision);
    }

    public static Script createScript(String fileName) {
        return createScript(fileName, (ScriptIndexes) null);
    }

    public static Script createScript(String fileName, ScriptIndexes baseLineRevision) {
        return new Script(fileName, 1L, "xxxxx", "@", "#", Collections.<Qualifier>emptySet(), asSet(new Qualifier("patch")), "postprocessing", baseLineRevision);
    }

    public static Script createScript(String fileName, String content) {
        return new Script(fileName, 1L, new ScriptContentHandle.StringScriptContentHandle(content, "ISO-8859-1"), "@", "#",
                Collections.<Qualifier>emptySet(), singleton(new Qualifier("patch")), "postprocessing", null);
    }

    public static FileSystemScriptLocation createFileSystemLocation(File scriptRootLocation) {
        return new FileSystemScriptLocation(scriptRootLocation, "ISO-8859-1", "postprocessing", Collections.<Qualifier>emptySet(),
                asSet(new Qualifier("patch")), "#", "@", asSet("sql"), null);
    }


    public static ScriptRepository getScriptRepository(SortedSet<Script> scriptsToReturn) {
        ScriptLocation scriptLocation = new ArchiveScriptLocation(scriptsToReturn, null, null, null, null, null, null, null, null);
        QualifierEvaluator qualifierEvaluator = getTrivialQualifierEvaluator();
        return new ScriptRepository(asSet(scriptLocation), qualifierEvaluator);
    }


    public static QualifierEvaluator getTrivialQualifierEvaluator() {
        return new QualifierEvaluator() {
            public boolean evaluate(Set<Qualifier> qualifiers) {
                return true;
            }
        };
    }

    public static ExecutedScriptInfoSource getExecutedScriptInfoSource(final SortedSet<ExecutedScript> executedScripts) {
        return new ExecutedScriptInfoSource() {

            public void registerExecutedScript(ExecutedScript executedScript) {
            }

            public void updateExecutedScript(ExecutedScript executedScript) {
            }

            public void clearAllExecutedScripts() {
            }

            public void deleteExecutedScript(ExecutedScript executedScript) {
            }

            public void renameExecutedScript(ExecutedScript executedScript, Script renamedToScript) {
            }

            public void deleteAllExecutedPostprocessingScripts() {
            }

            public void markErrorScriptsAsSuccessful() {
            }

            public void removeErrorScripts() {
            }

            public Set<ExecutedScript> getExecutedScripts() {
                return executedScripts;
            }
        };
    }
}
