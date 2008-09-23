/*
 * Copyright 2006-2008,  Unitils.org
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
 *
 * $Id$
 */
package org.dbmaintain.script.impl;

import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptSource;
import org.dbmaintain.script.impl.jar.DbScriptJarReader;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.PropertyUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Alexander Snaps <alex.snaps@gmail.com>
 * @author Filip Neven
 */
public class JarScriptSource extends DefaultScriptSource implements ScriptSource {

    public static final String DB_MAINTAINER_SCRIPT_JAR = "dbMaintainer.script.jar";

    @Override
	protected List<Script> loadAllScripts() {
    	String sourceJar = PropertyUtils.getString(DB_MAINTAINER_SCRIPT_JAR, configuration);
    	String encoding = PropertyUtils.getString(PROPKEY_SCRIPTS_ENCODING, configuration);
    	String targetDatabasePrefix = PropertyUtils.getString(PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX, configuration);
    	try {
        	List<Script> allScripts = new ArrayList<Script>();
            DbScriptJarReader jarReader = new DbScriptJarReader(sourceJar, encoding, targetDatabasePrefix);
            for (Script script : jarReader) {
            	if (isScriptFile(new File(script.getFileName()))) {
            	    allScripts.add(script);
            	}
            }
            return allScripts;
        } catch(IOException e) {
            throw new DbMaintainException("Error parsing JAR file " + sourceJar + " for scripts", e);
        }
	}

}
