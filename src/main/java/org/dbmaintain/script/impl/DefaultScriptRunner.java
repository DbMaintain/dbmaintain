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
package org.dbmaintain.script.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptParser;
import org.dbmaintain.script.ScriptParserFactory;
import org.dbmaintain.script.ScriptRunner;
import org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils;
import org.dbmaintain.util.DbMaintainException;

import java.io.Reader;
import java.util.Map;

/**
 * Default implementation of a script runner.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultScriptRunner implements ScriptRunner {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultScriptRunner.class);

    protected DbSupport defaultDbSupport;
    
    protected Map<String, DbSupport> nameDbSupportMap;
    
    protected SQLHandler sqlHandler;
    
    protected ScriptParserFactory scriptParserFactory;
    
    
    /**
     * Constructor for DefaultScriptRunner.
     * @param scriptParserFactory
     * @param defaultDbSupport
     * @param nameDbSupportMap
     * @param sqlHandler 
     */
    public DefaultScriptRunner(ScriptParserFactory scriptParserFactory, DbSupport defaultDbSupport, 
            Map<String, DbSupport> nameDbSupportMap, SQLHandler sqlHandler) {

        this.scriptParserFactory = scriptParserFactory;
        this.defaultDbSupport = defaultDbSupport;
        this.nameDbSupportMap = nameDbSupportMap;
        this.sqlHandler = sqlHandler;
    }


    /**
     * Executes the given script.
     * <p/>
     * All statements should be separated with a semicolon (;). The last statement will be
     * added even if it does not end with a semicolon.
     *
     * @param script The script, not null
     */
    public void execute(Script script) {

        Reader scriptContentReader = null;
        try {
            // Define the target database on which to execute the script
            DbSupport targetDbSupport = getTargetDatabaseDbSupport(script);
            if (targetDbSupport == null) {
                logger.info("Script " + script.getFileName() + " has target database " + script.getTargetDatabaseName() +
                    ". This database is disabled, so the script is not executed.");
                return;
            }
                
            // get content stream
            scriptContentReader = script.getScriptContentHandle().openScriptContentReader();
            // create a script parser for the target database in question 
            ScriptParser scriptParser = scriptParserFactory.createScriptParser(targetDbSupport
                    .getDatabaseDialect(), scriptContentReader);
            // parse and execute the statements
            String statement;
            while ((statement = scriptParser.getNextStatement()) != null) {
                sqlHandler.executeUpdate(statement, targetDbSupport.getDataSource());
            }
        } finally {
            IOUtils.closeQuietly(scriptContentReader);
        }
    }


    /**
     * @param script
     * @return
     */
    protected DbSupport getTargetDatabaseDbSupport(Script script) {
        if (script.getTargetDatabaseName() == null) {
        	return defaultDbSupport;
        } 
    	if (!nameDbSupportMap.containsKey(script.getTargetDatabaseName())) {
    		throw new DbMaintainException("Error executing script " + script.getFileName() + 
    				". No database initialized with the name " + script.getTargetDatabaseName());
    	}
    	return nameDbSupportMap.get(script.getTargetDatabaseName());
    }

}
