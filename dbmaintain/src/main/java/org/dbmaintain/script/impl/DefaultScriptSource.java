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
import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptContainer;
import org.dbmaintain.script.ScriptSource;
import org.dbmaintain.util.DbMaintainException;

import java.util.*;

/**
 * Implementation of {@link ScriptSource} that reads script files from the filesystem. <p/> Script
 * files should be located in the directory configured by scriptLocations.
 * Valid script files start with a version number followed by an underscore, and end with the
 * extension configured by scriptFileExtensions.
 * <p/>
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultScriptSource implements ScriptSource {

    /* Logger instance for this class */
    private static final Log logger = LogFactory.getLog(DefaultScriptSource.class);

    protected boolean useScriptFileLastModificationDates;

    protected boolean fixScriptOutOfSequenceExecutionAllowed;

    protected Set<ScriptContainer> scriptContainers;

    protected Set<String> scriptFileExtensions;

    protected List<Script> allUpdateScripts, allPostProcessingScripts;


    /**
     * Constructor for DefaultScriptSource.
     *
     * @param scriptContainers
     * @param useScriptFileLastModificationDates
     *
     * @param scriptFileExtensions
     */
    public DefaultScriptSource(Set<ScriptContainer> scriptContainers, boolean useScriptFileLastModificationDates,
                               Set<String> scriptFileExtensions, boolean fixScriptOutOfSequenceExecutionAllowed) {
        this.useScriptFileLastModificationDates = useScriptFileLastModificationDates;
        this.scriptContainers = scriptContainers;
        this.scriptFileExtensions = scriptFileExtensions;
        this.fixScriptOutOfSequenceExecutionAllowed = fixScriptOutOfSequenceExecutionAllowed;
        assertValidScriptExtensions();
    }


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
        List<Script> incrementalScripts = new ArrayList<Script>();
        for (Script script : scripts) {
            if (script.isIncremental()) {
                incrementalScripts.add(script);
            }
        }
        return incrementalScripts;
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
     *
     * @param currentVersion The start version, not null
     * @return The scripts that have a higher index of timestamp than the start version, not null.
     */
    public List<Script> getNewScripts(ScriptIndexes currentVersion, Set<ExecutedScript> alreadyExecutedScripts) {
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
            if (!script.isIncremental() && !alreadyExecutedScript.isScriptContentEqualTo(script, useScriptFileLastModificationDates)) {
                logger.info("Contents of script " + script.getFileName() + " have changed since the last database update: "
                        + script.getCheckSum());
                result.add(script);
            }
        }
        return result;
    }


    /**
     * Returns true if one or more scripts that have a version index equal to or lower than
     * the index specified by the given version object has been modified since the timestamp specified by
     * the given version.
     *
     * @param currentVersion The current database version, not null
     * @return True if an existing script has been modified, false otherwise
     */
    public boolean isIncrementalScriptModified(ScriptIndexes currentVersion, Set<ExecutedScript> alreadyExecutedScripts) {
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
                    if (indexedScript.isPatchScript()) {
                        if (!fixScriptOutOfSequenceExecutionAllowed) {
                            logger.warn("Found a new hoftix script that has a lower index than a script that has already been executed: " + indexedScript.getFileName());
                            return true;
                        }
                        logger.info("Found a new hoftix script that has a lower index than a script that has already been executed. Allowing the hotfix script to be executed out of sequence: " + indexedScript.getFileName());
                        return false;
                    }

                    logger.warn("Found a new script that has a lower index than a script that has already been executed: " + indexedScript.getFileName());
                    return true;
                }
                if (!alreadyExecutedScript.isScriptContentEqualTo(indexedScript, useScriptFileLastModificationDates)) {
                    logger.warn("Script found of which the contents have changed: " + indexedScript.getFileName());
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * Gets the configured post-processing script files and verifies that they on the file system. If one of them
     * doesn't exist or is not a file, an exception is thrown.
     *
     * @return All the postprocessing scripts, not null
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
            if (!isConfiguredExtension(script)) {
                continue;
            }
            if (script.isPostProcessingScript()) {
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
     * @param script
     * @return
     */
    protected boolean isConfiguredExtension(Script script) {
        for (String extension : scriptFileExtensions) {
            if (script.getFileName().endsWith(extension)) {
                return true;
            }
        }
        return false;
    }


    /**
     * @return A List containing all scripts in the given script locations, not null
     */
    protected List<Script> loadAllScripts() {
        List<Script> scripts = new ArrayList<Script>();
        for (ScriptContainer scriptContainer : scriptContainers) {
            scripts.addAll(scriptContainer.getScripts());
        }
        return scripts;
    }


    protected Map<String, Script> convertToScriptNameScriptMap(Set<ExecutedScript> executedScripts) {
        Map<String, Script> scriptMap = new HashMap<String, Script>();
        for (ExecutedScript executedScript : executedScripts) {
            scriptMap.put(executedScript.getScript().getFileName(), executedScript.getScript());
        }
        return scriptMap;
    }

    protected void assertValidScriptExtensions() {
        // check whether an extension is configured
        if (scriptFileExtensions.isEmpty()) {
            throw new DbMaintainException("No script file extensions specified!");
        }
        // Verify the correctness of the script extensions
        for (String extension : scriptFileExtensions) {
            if (extension.startsWith(".")) {
                throw new DbMaintainException("Script file extension " + extension + " should not start with a '.'");
            }
        }
    }

}
