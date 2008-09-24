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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContentHandle;
import org.dbmaintain.script.ScriptSource;
import org.dbmaintain.util.BaseConfigurable;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.FileUtils;
import org.dbmaintain.util.PropertyUtils;
import org.dbmaintain.version.Version;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link ScriptSource} that reads script files from the filesystem. <p/> Script
 * files should be located in the directory configured by {@link #PROPKEY_SCRIPT_LOCATIONS}.
 * Valid script files start with a version number followed by an underscore, and end with the
 * extension configured by {@link #PROPKEY_SCRIPT_EXTENSIONS}.
 * <p/>
 * todo refactor -> this is not a database task
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultScriptSource extends BaseConfigurable implements ScriptSource {

    /* Logger instance for this class */
    private static final Log logger = LogFactory.getLog(DefaultScriptSource.class);

    /**
     * Property key for the directory in which the script files are located
     */
    public static final String PROPKEY_SCRIPTS_LOCATION = "dbMaintainer.script.locations";

    /**
     * Property key for the extension of the script files
     */
    public static final String PROPKEY_SCRIPT_EXTENSIONS = "dbMaintainer.script.fileExtensions";

    /**
     * Property key for the directory in which the code script files are located
     */
    public static final String PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME = "dbMaintainer.postProcessingScript.directoryName";

    public static final String PROPKEY_USESCRIPTFILELASTMODIFICATIONDATES = "dbMaintainer.useScriptFileLastModificationDates.enabled";
    
    public static final String PROPKEY_SCRIPTS_ENCODING = "dbMaintainer.script.encoding";
    
    public static final String PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX = "dbMaintainer.script.targetDatabase.prefix";
    
    protected List<Script> allUpdateScripts, allPostProcessingScripts;


     /**
     * Gets a list of all available update scripts. These scripts can be used to completely recreate the
     * database from scratch, not null.
     * <p/>
     * The scripts are returned in the order in which they should be executed.
     *
     * @return all available database update scripts, not null
     */
    public List<Script> getAllUpdateScripts() {
    	if (allUpdateScripts == null) {
    		loadAndOrganizeAllScripts();
    	}
        return allUpdateScripts;
    }


    /**
     * @return All scripts that are incremental, i.e. non-repeatable, i.e. whose file name starts with an index
     */
    protected List<Script> getIncrementalScripts() {
    	List<Script> scripts = getAllUpdateScripts();
    	List<Script> indexedScripts = new ArrayList<Script>();
    	for (Script script : scripts) {
    		if (script.isIncremental()) {
    			indexedScripts.add(script);
    		}
    	}
    	return indexedScripts;
    }


    /**
     * Asserts that, in the given list of database update scripts, there are no two indexed scripts with the same version.
     * 
     * @param scripts The list of scripts, must be sorted by version
     */
    protected void assertNoDuplicateIndexes(List<Script> scripts) {
    	for (int i = 0; i < scripts.size() - 1; i++) {
    		Script script1 = scripts.get(i);
			Script script2 = scripts.get(i + 1);
			if (script1.isIncremental() && script2.isIncremental() && script1.getVersion().equals(script2.getVersion())) {
    			throw new DbMaintainException("Found 2 database scripts with the same version index: " 
    					+ script1.getFileName() + " and " + script2.getFileName() + " both have version index " 
    					+ script1.getVersion().getIndexesString());
    		}
    	}
    }


	/**
     * Returns a list of scripts with a higher version or whose contents were changed.
     * <p/>
     * The scripts are returned in the order in which they should be executed.
	 * @param currentVersion The start version, not null
     *
     * @return The scripts that have a higher index of timestamp than the start version, not null.
     */
    public List<Script> getNewScripts(Version currentVersion, Set<ExecutedScript> alreadyExecutedScripts) {
        Map<String, Script> alreadyExecutedScriptMap = convertToScriptNameScriptMap(alreadyExecutedScripts);
    	
    	List<Script> result = new ArrayList<Script>();

        List<Script> allScripts = getAllUpdateScripts();
        for (Script script : allScripts) {
        	Script alreadyExecutedScript = alreadyExecutedScriptMap.get(script.getFileName());
        	
            // If the script is indexed and the version is higher than the highest one currently applied to the database,
            // add it to the list.
            if (script.isIncremental() && script.getVersion().compareTo(currentVersion) > 0) {
				result.add(script);
				continue;
            }
            // Add the script if it's not indexed and if it wasn't yet executed
            if (!script.isIncremental() && alreadyExecutedScript == null) {
                result.add(script);
                continue;
            }
            // Add the script if it's not indexed and if it's contents have changed
            if (!script.isIncremental() && !alreadyExecutedScript.isScriptContentEqualTo(script, useScriptFileLastModificationDates())) {
            	logger.info("Contents of script " + script.getFileName() + " have changed since the last database update: "
            		 + script.getCheckSum());
            	result.add(script);
            }
        }
        return result;
    }


    /**
     * Returns true if one or more scripts that have a version index equal to or lower than
     * the index specified by the given version object has been modified since the timestamp specfied by
     * the given version.
     *
     * @param currentVersion The current database version, not null
     * @return True if an existing script has been modified, false otherwise
     */
    public boolean isExistingIndexedScriptModified(Version currentVersion, Set<ExecutedScript> alreadyExecutedScripts) {
    	Map<String, Script> alreadyExecutedScriptMap = convertToScriptNameScriptMap(alreadyExecutedScripts);
    	List<Script> incrementalScripts = getIncrementalScripts();
    	// Search for indexed scripts that have been executed but don't appear in the current indexed scripts anymore
    	for (ExecutedScript alreadyExecutedScript : alreadyExecutedScripts) {
    		if (alreadyExecutedScript.getScript().isIncremental() && Collections.binarySearch(incrementalScripts, alreadyExecutedScript.getScript()) < 0) {
    			logger.warn("Existing indexed script found that was executed, which has been removed: " + alreadyExecutedScript.getScript().getFileName());
    			return true;
    		}
    	}
    	
    	// Search for indexed scripts whose version < the current version, which are new or whose contents have changed
        for (Script indexedScript : incrementalScripts) {
            if (indexedScript.getVersion().compareTo(currentVersion) <= 0) {
            	Script alreadyExecutedScript = alreadyExecutedScriptMap.get(indexedScript.getFileName());
                if (alreadyExecutedScript == null) {
                	logger.warn("New index script has been added, with at least one already executed script having an higher index." + indexedScript.getFileName());
                	return true;
                }
                if (!alreadyExecutedScript.isScriptContentEqualTo(indexedScript, useScriptFileLastModificationDates())) {
                	logger.warn("Script found of which the contents have changed: " + indexedScript.getFileName());
                	return true;
                }
            }
        }
        return false;
    }
    
    
    protected boolean useScriptFileLastModificationDates() {
    	return PropertyUtils.getBoolean(PROPKEY_USESCRIPTFILELASTMODIFICATIONDATES, configuration);
    }


    /**
     * Gets the configured post-processing script files and verfies that they on the file system. If one of them
     * doesn't exist or is not a file, an exception is thrown.
     *
     * @return All the postprocessing code scripts, not null
     */
    public List<Script> getPostProcessingScripts() {
    	if (allPostProcessingScripts == null) {
    		loadAndOrganizeAllScripts();
    	}
        return allPostProcessingScripts;
    }
    
    
    /**
     * Loads all scripts and organizes them: Splits them into update and postprocessing scripts, sorts
     * them in their execution order, and makes sure there are no 2 update or postprocessing scripts with 
     * the same index.
     */
    protected void loadAndOrganizeAllScripts() {
    	List<Script> allScripts = loadAllScripts();
    	allUpdateScripts = new ArrayList<Script>();
    	allPostProcessingScripts = new ArrayList<Script>();
    	for (Script script : allScripts) {
    		if (isPostProcessingScript(script)) {
    			allPostProcessingScripts.add(script);
    		} else {
    			allUpdateScripts.add(script);
    		}
    	}
		Collections.sort(allUpdateScripts);
		assertNoDuplicateIndexes(allUpdateScripts);
		Collections.sort(allPostProcessingScripts);
		assertNoDuplicateIndexes(allPostProcessingScripts);
    	
    }


    /**
     * @return A List containing all scripts in the given script locations, not null
     */
	protected List<Script> loadAllScripts() {
		String scriptsLocation = PropertyUtils.getString(PROPKEY_SCRIPTS_LOCATION, configuration);
		if (!new File(scriptsLocation).exists()) {
            throw new DbMaintainException("File location " + scriptsLocation + " defined in property " + PROPKEY_SCRIPTS_LOCATION + " doesn't exist");
        }
		List<Script> scripts = new ArrayList<Script>();
		getScriptsAt(scripts, scriptsLocation, "");
		return scripts;
	}

    
    /**
     * Adds all scripts available in the given directory or one of its subdirectories to the
     * given List of files
     *
     * @param scriptLocation       The current script location, not null
     * @param currentParentIndexes The indexes of the current parent folders, not null
     * @param scriptFiles          The list to which the available script have to be added
     */
    protected void getScriptsAt(List<Script> scripts, String scriptRoot, String relativeLocation) {
        File currentLocation = new File(scriptRoot + "/" + relativeLocation);
    	if (currentLocation.isFile() && isScriptFile(currentLocation)) {
            Script script = createScript(currentLocation, relativeLocation);
            scripts.add(script);
            return;
        }
        // recursively scan sub folders for script files
        if (currentLocation.isDirectory()) {
            for (File subLocation : currentLocation.listFiles()) {
                getScriptsAt(scripts, scriptRoot, 
                		"".equals(relativeLocation) ? subLocation.getName() : relativeLocation + '/' + subLocation.getName());
            }
        }
    }


    /**
     * @param script A database script, not null
     * @return True if the given script is a post processing script according to the script source configuration
     */
    protected boolean isPostProcessingScript(Script script) {
    	String postProcessingScriptDirName = PropertyUtils.getString(PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, configuration);
    	if (StringUtils.isEmpty(postProcessingScriptDirName)) {
    		return false;
    	}
		return script.getFileName().startsWith(postProcessingScriptDirName + '/') ||
		    script.getFileName().startsWith(postProcessingScriptDirName + '\\');
	}


	/**
     * Indicates if the given file is a database update script file
     *
     * @param file The file, not null
     * @return True if the given file is a database update script file
     */
    protected boolean isScriptFile(File file) {
        String name = file.getName();
        for (String fileExtension : getScriptExtensions()) {
            if (name.endsWith(fileExtension)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Creates a script object for the given script file
     *
     * @param scriptFile The script file, not null
     * @return The script, not null
     */
    protected Script createScript(File scriptFile, String relativePath) {
        return new Script(relativePath, scriptFile.lastModified(), 
                new ScriptContentHandle.UrlScriptContentHandle(FileUtils.getUrl(scriptFile), 
                PropertyUtils.getString(PROPKEY_SCRIPTS_ENCODING, configuration)),
                PropertyUtils.getString(PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX, configuration));
    }


    /**
     * Gets the configured script locations and verifies that they on the file system. If one of them
     * doesn't exist, an exception is thrown.
     *
     * @return The files, not null
     */
    protected File getScriptsLocation() {
        String location = PropertyUtils.getString(PROPKEY_SCRIPTS_LOCATION, configuration);
        File locationFile = new File(location);
        if (!locationFile.exists()) {
            throw new DbMaintainException("File location " + location + " defined in property " + PROPKEY_SCRIPTS_LOCATION + " doesn't exist");
        }
		return locationFile;
    }


    /**
     * Gets the configured extensions for the script files.
     *
     * @return The extensions, not null
     */
    protected List<String> getScriptExtensions() {
        List<String> extensions = PropertyUtils.getStringList(PROPKEY_SCRIPT_EXTENSIONS, configuration);

        // check whether an extension is configured
        if (extensions.isEmpty()) {
            logger.warn("No extensions are specificied using the property " + PROPKEY_SCRIPT_EXTENSIONS + ". The Unitils database maintainer won't do anyting");
        }
        // Verify the correctness of the script extensions
        for (String extension : extensions) {
            if (extension.startsWith(".")) {
                throw new DbMaintainException("DefaultScriptSource file extension defined by " + PROPKEY_SCRIPT_EXTENSIONS + " should not start with a '.'");
            }
        }
        return extensions;
    }


    /**
     * Verifies that directories and files in the given list of fileLocations exist on the file
     * system. If one of them doesn't exist, an exception is thrown
     *
     * @param locations    The directories and files that need to be checked
     * @param propertyName The name of the property, for the error message if a location does not exist
     * @return The list of files, not null
     */
    protected List<File> getFiles(List<String> locations, String propertyName) {
        List<File> result = new ArrayList<File>();
        for (String fileLocation : locations) {
            File file = new File(fileLocation);
            if (!file.exists()) {
                throw new DbMaintainException("File location " + fileLocation + " defined in property " + propertyName + " doesn't exist");
            }
            result.add(file);
        }
        return result;
    }
    
    
    protected Map<String, Script> convertToScriptNameScriptMap(Set<ExecutedScript> executedScripts) {
		Map<String, Script> scriptMap = new HashMap<String, Script>();
        for (ExecutedScript executedScript : executedScripts) {
    		scriptMap.put(executedScript.getScript().getFileName(), executedScript.getScript());
        }
		return scriptMap;
	}

}
