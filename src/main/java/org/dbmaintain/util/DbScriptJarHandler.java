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

import org.dbmaintain.DBMaintainer;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.script.ScriptSource;
import org.dbmaintain.script.impl.DefaultScriptSource;
import org.dbmaintain.script.impl.JarScriptSource;
import org.dbmaintain.version.impl.DefaultExecutedScriptInfoSource;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 *
 */
public abstract class DbScriptJarHandler {

	protected DbSupport defaultDbSupport;
	protected Map<String, DbSupport> nameDbSupportMap;
	protected String extensions;
	
	
	protected DbScriptJarHandler(DbSupport defaultDbSupport, Map<String, DbSupport> nameDbSupportMap, String extensions) {
		super();
		this.defaultDbSupport = defaultDbSupport;
		this.nameDbSupportMap= nameDbSupportMap;
		this.extensions = extensions;
	}

	/**
	 * Constitutes the configuration of the {@link DBMaintainer}. Some configuration is hard coded (no from-scratch
	 * updates, no test database post-processing such as disabling of constraints), some is retrieved from the jar
	 * file (organization of scripts) and some is configured in this class (database dialect and schema name)
	 * 
	 * @param jarFileName
	 * @return
	 */
	protected Properties getDbMaintainerConfiguration(String jarFileName) {
		Properties configuration = new DbMaintainConfigurationLoader().getDefaultConfiguration();
		
		// Initialize the script organization properties, which are read from the properties file that is packaged
        // with the jar
        configuration.putAll(getDbScriptConfig(jarFileName));
        if (extensions != null) {
            configuration.put(DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS, extensions);
        }
        // Make sure that from-scratch updates are disabled, and that no post-processing is performed on the
		// target database
		configuration.put(DBMaintainer.PROPKEY_DISABLE_CONSTRAINTS_ENABLED, Boolean.toString(false));
		configuration.put(DBMaintainer.PROPKEY_DB_CLEANER_ENABLED, Boolean.toString(false));
		configuration.put(DBMaintainer.PROPKEY_UPDATE_SEQUENCES_ENABLED, Boolean.toString(false));
		configuration.put(DBMaintainer.PROPKEY_FROM_SCRATCH_ENABLED, Boolean.toString(false));
		configuration.put(DBMaintainer.PROPKEY_KEEP_RETRYING_AFTER_ERROR_ENABLED, Boolean.toString(false));
		configuration.put(DBMaintainer.PROPKEY_GENERATE_DATA_SET_STRUCTURE_ENABLED, Boolean.toString(false));
		configuration.put(DefaultScriptSource.PROPKEY_USESCRIPTFILELASTMODIFICATIONDATES, Boolean.toString(false));
		configuration.put(DefaultExecutedScriptInfoSource.PROPERTY_AUTO_CREATE_EXECUTED_SCRIPTS_TABLE, Boolean.toString(false));
	
		// Make sure the JarScriptSource is used, that reads scripts from the jar file instead of the file system
		configuration.put(ScriptSource.class.getName() + ".implClassName", JarScriptSource.class.getName());
		configuration.put(JarScriptSource.DB_MAINTAINER_SCRIPT_JAR, jarFileName);
		
		return configuration;
	}

	/**
	 * @param jarFileName The path to the jar file
	 * @return The properties packaged with the jar file
	 */
	private Properties getDbScriptConfig(String jarFileName) {
		try {
			URL dbScriptConfigUrl = getJarEntryURL(jarFileName, DbScriptJarCreator.DBSCRIPT_JAR_PROPERTIES_FILENAME);
			Properties dbScriptConfig = new Properties();
			dbScriptConfig.load(dbScriptConfigUrl.openStream());
			return dbScriptConfig;
		} catch (IOException e) {
			throw new DbMaintainException("Error parsing jar file " + jarFileName, e);
		}
	}

	/**
	 * @param jarFileName The path to the jar file
	 * @param jarEntryName The name of the jar entry
	 * @return An URL that points directly to the given entry in the given jar file
	 */
	private URL getJarEntryURL(String jarFileName, String jarEntryName) {
	    try {
	        return new URL(new StringBuilder("jar:file:")
	                .append(jarFileName)
	                .append("!/")
	                .append(jarEntryName)
	                .toString());
	    } catch(MalformedURLException e) {
	        throw new DbMaintainException("Error creating URL out of script " + jarEntryName + " of jar file " + jarFileName);
	    }
	}

}
