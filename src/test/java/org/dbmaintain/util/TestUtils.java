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
import org.dbmaintain.script.impl.DefaultScriptParser;
import org.dbmaintain.script.impl.DefaultScriptParserFactory;
import org.dbmaintain.script.impl.DefaultScriptRunner;
import org.dbmaintain.script.impl.DefaultScriptSource;
import org.dbmaintain.script.impl.FileScriptContainer;
import org.dbmaintain.structure.impl.DefaultConstraintsDisabler;
import org.dbmaintain.structure.impl.DefaultSequenceUpdater;
import org.dbmaintain.version.impl.DefaultExecutedScriptInfoSource;

import javax.sql.DataSource;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 *
 */
public class TestUtils {

	public static DbSupport getDbSupport() {
	    return getDbSupport("PUBLIC");
	}


	public static DbSupport getDbSupport(String... schemaNames) {
        DataSource dataSource = getDataSource();
		DbSupport dbSupport = new HsqldbDbSupport(null, dataSource, schemaNames[0], 
		        CollectionUtils.asSet(schemaNames), new DefaultSQLHandler(), null, null);
		return dbSupport;
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
		DefaultDBCleaner defaultDbCleaner = new DefaultDBCleaner(getNameDbSupportMap(dbSupport), new DefaultSQLHandler());
		return defaultDbCleaner;
	}
	
	
	public static DefaultScriptSource getDefaultScriptSource(String scriptLocation, boolean useScriptFileLastModificationDates) {
	    Set<String> scriptFileExtensions = CollectionUtils.asSet("sql");
        ScriptContainer scriptContainer = new FileScriptContainer(new File(scriptLocation), 
	            scriptFileExtensions, "@", "postprocessing", "ISO-8859-1");
        DefaultScriptSource defaultScriptSource = new DefaultScriptSource(
                CollectionUtils.asSet(scriptContainer), useScriptFileLastModificationDates, scriptFileExtensions);
        return defaultScriptSource;
    }
	
	
	public static DefaultScriptRunner getDefaultScriptRunner(DbSupport dbSupport) {
	    Map<String, Class<? extends ScriptParser>> databaseDialectScriptParserClassMap = new HashMap<String, Class<? extends ScriptParser>>();
	    databaseDialectScriptParserClassMap.put("hsqldb", DefaultScriptParser.class);
	    ScriptParserFactory scriptParserFactory = new DefaultScriptParserFactory(databaseDialectScriptParserClassMap, false);
		DefaultScriptRunner defaultScriptRunner = new DefaultScriptRunner(scriptParserFactory, dbSupport, getNameDbSupportMap(dbSupport), new DefaultSQLHandler());
		return defaultScriptRunner;
	}
	
	public static DefaultConstraintsDisabler getDefaultConstraintsDisabler(DbSupport dbSupport) {
		DefaultConstraintsDisabler defaultConstraintsDisabler = new DefaultConstraintsDisabler(CollectionUtils.asSet(dbSupport));
		return defaultConstraintsDisabler;
	}
	
	
	public static DefaultSequenceUpdater getDefaultSequenceUpdater(DbSupport dbSupport) {
		DefaultSequenceUpdater defaultSequenceUpdater = new DefaultSequenceUpdater(1000L, CollectionUtils.asSet(dbSupport));
		return defaultSequenceUpdater;
	}
	
	
	public static DefaultExecutedScriptInfoSource getDefaultExecutedScriptInfoSource(DbSupport dbSupport, boolean autoCreateExecutedScriptsTable) {
		DefaultExecutedScriptInfoSource defaultExecutedScriptInfoSource = new DefaultExecutedScriptInfoSource(autoCreateExecutedScriptsTable,
		        "db_executed_scripts", "script_file_name", 100, "version", 100, "last_modified_at", "checksum", 150, "executed_at", 10, "succeeded",
		        new SimpleDateFormat("dd/MM/yyyy"), dbSupport, new DefaultSQLHandler(), "@"); 
		return defaultExecutedScriptInfoSource;
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
