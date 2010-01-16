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

import org.dbmaintain.clean.impl.DefaultDBCleaner;
import org.dbmaintain.clear.impl.DefaultDBClearer;
import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.dbsupport.DbItemIdentifier;
import org.dbmaintain.dbsupport.DbItemType;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.dbsupport.impl.HsqldbDbSupport;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.executedscriptinfo.impl.DefaultExecutedScriptInfoSource;
import org.dbmaintain.script.*;
import org.dbmaintain.script.impl.DefaultScriptRunner;
import org.dbmaintain.script.impl.FileSystemScriptLocation;
import org.dbmaintain.script.impl.ScriptLocation;
import org.dbmaintain.script.impl.ScriptRepository;
import org.dbmaintain.scriptparser.ScriptParserFactory;
import org.dbmaintain.scriptparser.impl.DefaultScriptParserFactory;
import org.dbmaintain.structure.impl.DefaultConstraintsDisabler;
import org.dbmaintain.structure.impl.DefaultSequenceUpdater;

import javax.sql.DataSource;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.dbmaintain.dbsupport.DbMaintainDataSource.createDataSource;
import static org.dbmaintain.util.CollectionUtils.asSet;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public abstract class TestUtils {


    public static DbSupport getDbSupport() {
        return getDbSupport("PUBLIC");
    }

    public static DatabaseInfo getHsqlDatabaseInfo(String... schemaNames) {
        if (schemaNames == null || schemaNames.length == 0) {
            schemaNames = new String[]{"PUBLIC"};
        }
        return new DatabaseInfo(null, null, "org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:unitils", "sa", "", asList(schemaNames));
    }

    public static DbSupport getDbSupport(String... schemaNames) {
        DatabaseInfo databaseInfo = getHsqlDatabaseInfo(schemaNames);
        DataSource dataSource = createDataSource(databaseInfo);
        return new HsqldbDbSupport(databaseInfo, dataSource, new DefaultSQLHandler(), null, null);
    }


    public static DefaultDBClearer getDefaultDBClearer(DbSupport dbSupport) {
        return new DefaultDBClearer(getNameDbSupportMap(dbSupport));
    }


    public static DefaultDBCleaner getDefaultDBCleaner(DbSupport dbSupport) {
        return new DefaultDBCleaner(getNameDbSupportMap(dbSupport), Collections.<DbItemIdentifier>emptySet(), Collections.<DbItemIdentifier>emptySet(), new DefaultSQLHandler());
    }


    public static DefaultScriptRunner getDefaultScriptRunner(DbSupport dbSupport) {
        Map<String, ScriptParserFactory> databaseDialectScriptParserClassMap = new HashMap<String, ScriptParserFactory>();
        databaseDialectScriptParserClassMap.put("hsqldb", new DefaultScriptParserFactory(false));
        return new DefaultScriptRunner(databaseDialectScriptParserClassMap, dbSupport, getNameDbSupportMap(dbSupport), new DefaultSQLHandler());
    }


    public static DefaultConstraintsDisabler getDefaultConstraintsDisabler(DbSupport dbSupport) {
        return new DefaultConstraintsDisabler(asSet(dbSupport));
    }


    public static DefaultSequenceUpdater getDefaultSequenceUpdater(DbSupport dbSupport) {
        return new DefaultSequenceUpdater(1000L, asSet(dbSupport));
    }


    public static DefaultExecutedScriptInfoSource getDefaultExecutedScriptInfoSource(DbSupport dbSupport, boolean autoCreateExecutedScriptsTable) {
        return new DefaultExecutedScriptInfoSource(autoCreateExecutedScriptsTable,
                "db_executed_scripts", "script_file_name", 100, "last_modified_at", "checksum", 150, "executed_at", 10, "succeeded",
                new SimpleDateFormat("dd/MM/yyyy"), dbSupport, new DefaultSQLHandler(), "@", "#", Collections.<Qualifier>emptySet(),
                asSet(new Qualifier("patch")), "postprocessing");
    }


    public static Map<String, DbSupport> getNameDbSupportMap(DbSupport dbSupport) {
        Map<String, DbSupport> dbNameDbSupportMap = new HashMap<String, DbSupport>();
        dbNameDbSupportMap.put(null, dbSupport);
        return dbNameDbSupportMap;
    }

    public static Set<DbItemIdentifier> toDbItemIdentifiers(DbItemType dbItemType, Set<String> itemsAsString, DbSupport defaultDbSupport, Map<String, DbSupport> nameDbSupportMap) {
        Set<DbItemIdentifier> itemIdentifiers = new HashSet<DbItemIdentifier>();
        for (String itemAsString : itemsAsString) {
            itemIdentifiers.add(DbItemIdentifier.parseItemIdentifier(dbItemType, itemAsString, defaultDbSupport, nameDbSupportMap));
        }
        return itemIdentifiers;
    }


    public static Set<DbItemIdentifier> toDbSchemaIdentifiers(Set<String> schemasAsString, DbSupport defaultDbSupport, Map<String, DbSupport> nameDbSupportMap) {
        Set<DbItemIdentifier> schemaIdentifiers = new HashSet<DbItemIdentifier>();
        for (String schemaAsString : schemasAsString) {
            schemaIdentifiers.add(DbItemIdentifier.parseSchemaIdentifier(schemaAsString, defaultDbSupport, nameDbSupportMap));
        }
        return schemaIdentifiers;
    }

    public static Script createScript(String fileName) {
        return new Script(fileName, 1L, "xxxxx", "@", "#", Collections.<Qualifier>emptySet(), asSet(new Qualifier("patch")), "postprocessing");
    }

    public static Script createScript(String fileName, String content) {
        return new Script(fileName, 1L, new ScriptContentHandle.StringScriptContentHandle(content, "ISO-8859-1"), "@", "#",
                Collections.<Qualifier>emptySet(), singleton(new Qualifier("patch")), "postprocessing");
    }

    public static FileSystemScriptLocation createFileSystemLocation(File scriptRootLocation) {
        return new FileSystemScriptLocation(scriptRootLocation, "ISO-8859-1", "postprocessing", Collections.<Qualifier>emptySet(),
                asSet(new Qualifier("patch")), "#", "@", asSet("sql"));
    }

    public static ScriptRepository getScriptRepository(final SortedSet<Script> scriptsToReturn) {
        ScriptLocation scriptLocation = new ScriptLocation() {
            public String getLocationName() {
                return null;
            }

            @Override
            public SortedSet<Script> getScripts() {
                return scriptsToReturn;
            }
        };
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

            public Set<ExecutedScript> getExecutedScripts() {
                return executedScripts;
            }
        };
    }
}
