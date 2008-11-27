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
package org.dbmaintain.scriptparser.impl;

import java.io.Reader;
import java.util.Map;

import org.dbmaintain.scriptparser.ScriptParser;
import org.dbmaintain.scriptparser.ScriptParserFactory;
import org.dbmaintain.util.ReflectionUtils;


/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultScriptParserFactory implements ScriptParserFactory {

    protected Map<String, Class<? extends ScriptParser>> databaseDialectScriptParserClassMap;
    
    protected boolean backSlashEscapingEnabled;
    
    /**
     * Constructor for DefaultScriptParserFactory.
     * @param databaseDialectScriptParserClassMap
     * @param backSlashEscapingEnabled 
     */
    public DefaultScriptParserFactory(
            Map<String, Class<? extends ScriptParser>> databaseDialectScriptParserClassMap, 
            boolean backSlashEscapingEnabled) {
        this.databaseDialectScriptParserClassMap = databaseDialectScriptParserClassMap;
        this.backSlashEscapingEnabled = backSlashEscapingEnabled;
    }


    public ScriptParser createScriptParser(String databaseDialect, Reader scriptContentReader) {
        Class<? extends ScriptParser> scriptParserClass = databaseDialectScriptParserClassMap.get(databaseDialect);
        return ReflectionUtils.createInstanceOfType(scriptParserClass, false, 
                new Class<?>[] {Reader.class, boolean.class}, 
                new Object[] {scriptContentReader, backSlashEscapingEnabled});
    }

}
