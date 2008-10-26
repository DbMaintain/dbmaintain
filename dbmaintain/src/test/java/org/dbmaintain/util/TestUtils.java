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

import org.apache.commons.dbcp.BasicDataSource;
import org.dbmaintain.clean.impl.DefaultDBCleaner;
import org.dbmaintain.clean.impl.DefaultDBClearer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DefaultSQLHandler;
import org.dbmaintain.dbsupport.HsqldbDbSupport;
import org.dbmaintain.script.ScriptContainer;
import org.dbmaintain.script.ScriptParser;
import org.dbmaintain.script.ScriptParserFactory;
import org.dbmaintain.script.impl.*;
import org.dbmaintain.structure.impl.DefaultConstraintsDisabler;
import org.dbmaintain.structure.impl.DefaultSequenceUpdater;
import static org.dbmaintain.util.CollectionUtils.asSet;
import org.dbmaintain.version.impl.DefaultExecutedScriptInfoSource;

import javax.sql.DataSource;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * todo javadoc
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
abstract public class TestUtils {


    public static DbSupport getDbSupport() {
        return getDbSupport("PUBLIC");
    }


    public static DbSupport getDbSupport(String... schemaNames) {
        DataSource dataSource = getDataSource();
        return new HsqldbDbSupport(null, dataSource, schemaNames[0], asSet(schemaNames), new DefaultSQLHandler(), null, null);
    }


    protected static DataSource getDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
        dataSource.setUrl("jdbc:hsqldb:mem:unitils");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        return dataSource;
    }


    public static DefaultDBClearer getDefaultDBClearer(DbSupport dbSupport) {
        return new DefaultDBClearer(getNameDbSupportMap(dbSupport), Collections.<DbItemIdentifier>emptySet(),
                Collections.<DbItemIdentifier>emptySet(), Collections.<DbItemIdentifier>emptySet(), Collections.<DbItemIdentifier>emptySet(),
                Collections.<DbItemIdentifier>emptySet(), Collections.<DbItemIdentifier>emptySet(), Collections.<DbItemIdentifier>emptySet(),
                Collections.<DbItemIdentifier>emptySet());
    }


    public static DefaultDBCleaner getDefaultDBCleaner(DbSupport dbSupport) {
        return new DefaultDBCleaner(getNameDbSupportMap(dbSupport),
                Collections.<DbItemIdentifier>emptySet(), Collections.<DbItemIdentifier>emptySet(), new DefaultSQLHandler());
    }


    public static DefaultScriptSource getDefaultScriptSource(String scriptLocation, boolean useScriptFileLastModificationDates) {
        Set<String> scriptFileExtensions = asSet("sql");
        ScriptContainer scriptContainer = new FileScriptContainer(new File(scriptLocation), scriptFileExtensions, "@", "postprocessing", "fix", "ISO-8859-1");
        return new DefaultScriptSource(asSet(scriptContainer), useScriptFileLastModificationDates, scriptFileExtensions, false);
    }


    public static DefaultScriptRunner getDefaultScriptRunner(DbSupport dbSupport) {
        Map<String, Class<? extends ScriptParser>> databaseDialectScriptParserClassMap = new HashMap<String, Class<? extends ScriptParser>>();
        databaseDialectScriptParserClassMap.put("hsqldb", DefaultScriptParser.class);
        ScriptParserFactory scriptParserFactory = new DefaultScriptParserFactory(databaseDialectScriptParserClassMap, false);
        return new DefaultScriptRunner(scriptParserFactory, dbSupport, getNameDbSupportMap(dbSupport), new DefaultSQLHandler());
    }


    public static DefaultConstraintsDisabler getDefaultConstraintsDisabler(DbSupport dbSupport) {
        return new DefaultConstraintsDisabler(asSet(dbSupport));
    }


    public static DefaultSequenceUpdater getDefaultSequenceUpdater(DbSupport dbSupport) {
        return new DefaultSequenceUpdater(1000L, asSet(dbSupport));
    }


    public static DefaultExecutedScriptInfoSource getDefaultExecutedScriptInfoSource(DbSupport dbSupport, boolean autoCreateExecutedScriptsTable) {
        return new DefaultExecutedScriptInfoSource(autoCreateExecutedScriptsTable,
                "db_executed_scripts", "script_file_name", 100, "version", 100, "last_modified_at", "checksum", 150, "executed_at", 10, "succeeded",
                new SimpleDateFormat("dd/MM/yyyy"), dbSupport, new DefaultSQLHandler(), "fix", "@");
    }


    public static Map<String, DbSupport> getNameDbSupportMap(DbSupport dbSupport) {
        Map<String, DbSupport> dbNameDbSupportMap = new HashMap<String, DbSupport>();
        dbNameDbSupportMap.put(null, dbSupport);
        return dbNameDbSupportMap;
    }


    public static Set<DbItemIdentifier> toDbItemIdentifiers(Set<String> itemsAsString, DbSupport defaultDbSupport, Map<String, DbSupport> nameDbSupportMap) {
        Set<DbItemIdentifier> itemIdentifiers = new HashSet<DbItemIdentifier>();
        for (String itemAsString : itemsAsString) {
            itemIdentifiers.add(DbItemIdentifier.parseItemIdentifier(itemAsString, defaultDbSupport, nameDbSupportMap));
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

}
