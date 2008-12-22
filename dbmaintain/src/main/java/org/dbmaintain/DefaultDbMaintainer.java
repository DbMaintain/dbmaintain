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
package org.dbmaintain;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clear.DBClearer;
import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_KEEP_RETRYING_AFTER_ERROR_ENABLED;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.*;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.util.DbMaintainException;

import java.util.*;

/**
 * Class that offers operations for automatically maintaining a database.
 * <p/>
 * The {@link #updateDatabase()} operation can be used to bring the database to the latest version. The
 * {@link #markDatabaseAsUpToDate()} operation updates the state of the database to indicate that all scripts have been
 * executed, without actually executing them. {@link #clearDatabase()} will drop all tables and update the state to
 * indicate that no scripts have been executed yet on the database.
 * <p/>
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultDbMaintainer implements DbMaintainer {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultDbMaintainer.class);

    /**
     * Provider of the current version of the database, and means to increment it
     */
    protected ExecutedScriptInfoSource executedScriptInfoSource;

    /**
     * Provider of scripts for updating the database to a higher version
     */
    protected ScriptSource scriptSource;

    protected ScriptContainer scriptContainer;

    /**
     * Executer of the scripts
     */
    protected ScriptRunner scriptRunner;

    /**
     * Clearer of the database (removed all tables, sequences, ...) before updating
     */
    protected DBClearer dbClearer;

    /**
     * Cleaner of the database (deletes all data from all tables before updating
     */
    protected DBCleaner dbCleaner;

    /**
     * Disabler of constraints
     */
    protected ConstraintsDisabler constraintsDisabler;

    /**
     * Database sequence updater
     */
    protected SequenceUpdater sequenceUpdater;

    // todo javadoc
    protected boolean cleanDbEnabled;

    /**
     * Indicates whether updating the database from scratch is enabled. If true, the database is
     * cleared before updating if an already executed script is modified
     */
    protected boolean fromScratchEnabled;

    /**
     * Defines whether script last modification dates can be used to decide whether an existing script has changed. If set
     * to true, the dbmaintainer will decide that a file didn't change since the last time if it's last modification date hasn't
     * changed. If it did change, it will first calculate the checksum of the file to verify that the content really
     * changed. Setting this property to true improves performance: if set to false the checksum of every script must
     * be calculated for each run of the dbmaintainer.
     */
    protected boolean useScriptFileLastModificationDates;

    /**
     * If this property is set to true, a patch script is allowed to be executed even if another script with a higher index
     * was already executed.
      */
    protected boolean allowOutOfSequenceExecutionOfPatchScripts;

    /**
     * Indicates whether a from scratch update should be performed when the previous update failed,
     * but none of the scripts were modified since that last update. If true a new update will be
     * tried only when changes were made to the script files
     */
    protected boolean keepRetryingAfterError;

    /**
     * Indicates if foreign key and not null constraints should removed after updating the database
     * structure
     */
    protected boolean disableConstraintsEnabled;

    /**
     * Indicates whether sequences and identity columns must be updated to a certain minimal value
     */
    protected boolean updateSequencesEnabled;

    protected DefaultDbMaintainer() {
    }

    /**
     * Creates a new instance
     *
     * @param scriptRunner The runner that executes the database scripts
     * @param scriptSource
     * @param scriptContainer
     * @param executedScriptInfoSource Provides information about which scripts were already executed on the database
     * @param fromScratchEnabled If true, the database will be cleared and recreated from scratch if needed
     * @param keepRetryingAfterError If true and fromScratchEnabled == true, an attempt to recreate the database will
     * be performed if a script execution failed the last time, even if no changes were made to any script
     * @param cleanDbEnabled If true, the data from all tables will be removed before performing any updates
     * @param disableConstraintsEnabled If true, all foreign key and not null constraints will be automatically disabled
     * or removed after each update
     * @param updateSequencesEnabled If true, the value of all sequences will be set to a minimal value after each update
     * @param dbClearer Helper object that can clear the database, i.e. drop all database objects
     * @param dbCleaner Helper object that can clean the database, i.e. remove the data from all tables
     * @param constraintsDisabler Helper object that can disable or remove all foreign key or not null constraints
     * @param sequenceUpdater Helper object that can update all sequences to a minimal value
     */
    public DefaultDbMaintainer(ScriptRunner scriptRunner, ScriptSource scriptSource, ScriptContainer scriptContainer, ExecutedScriptInfoSource executedScriptInfoSource,
               boolean fromScratchEnabled, boolean useScriptFileLastModificationDates, boolean allowOutOfSequenceExecutionOfPatchScripts, boolean keepRetryingAfterError,
               boolean cleanDbEnabled, boolean disableConstraintsEnabled, boolean updateSequencesEnabled, DBClearer dbClearer,
               DBCleaner dbCleaner, ConstraintsDisabler constraintsDisabler, SequenceUpdater sequenceUpdater) {

        this.scriptRunner = scriptRunner;
        this.scriptSource = scriptSource;
        this.scriptContainer = scriptContainer;
        this.executedScriptInfoSource = executedScriptInfoSource;
        this.fromScratchEnabled = fromScratchEnabled;
        this.useScriptFileLastModificationDates = useScriptFileLastModificationDates;
        this.allowOutOfSequenceExecutionOfPatchScripts = allowOutOfSequenceExecutionOfPatchScripts;
        this.keepRetryingAfterError = keepRetryingAfterError;
        this.cleanDbEnabled = cleanDbEnabled;
        this.disableConstraintsEnabled = disableConstraintsEnabled;
        this.updateSequencesEnabled = updateSequencesEnabled;
        this.dbClearer = dbClearer;
        this.dbCleaner = dbCleaner;
        this.constraintsDisabler = constraintsDisabler;
        this.sequenceUpdater = sequenceUpdater;

        assertNoDuplicateScriptIndexes();
    }


    /**
     * This operation can be used to bring the database to the latest version. First it checks which scripts were already
     * applied to the database and executes the new scripts or the updated repeatable scripts. If an existing incremental
     * script was changed,  removed, or if a new incremental script has been added with a lower index than one that was
     * already executed, an error is given; unless the <fromScratch> option is enabled: in that case all database objects
     * are removed and the database is rebuilt from scratch. If there are post-processing scripts, these are always executed
     * at the end.
     */
    public void updateDatabase() {
        // check whether an from scratch update should be performed
        if (fromScratchEnabled && (executedScriptInfoSource.isFromScratchUpdateRecommended() || shouldUpdateDatabaseFromScratch())) {
            // From scratch needed, clear the database and retrieve scripts
            clearDatabase();
            // update database with all scripts
            updateDatabase(getAllUpdateScripts());
            return;
        }

        // perform an incremental update
        updateDatabase(getNewScripts());
    }


    /**
     * Asserts that, there are no two indexed scripts with the same version.
     */
    protected void assertNoDuplicateScriptIndexes() {
        Script previous, current = null;
        for (Script script : scriptContainer.getScripts()) {
            previous = current;
            current = script;
            if (previous != null && previous.isIncremental() && current.isIncremental() && previous.getVersion().equals(current.getVersion())) {
                throw new DbMaintainException("Found 2 database scripts with the same version index: "
                        + previous.getFileName() + " and " + current.getFileName() + " both have version index "
                        + previous.getVersion().getIndexesString());
            }
        }
    }


    /**
     * Returns a list of scripts including the ones that:
     * <ol><li>have a higher version than the given version</li>
     * <li>are unversioned, and they weren't yet applied on the database</li>
     * <li>are unversioned, and their contents differ from the one currently applied to the database</li>
     * <p/>
     * The scripts are returned in the order in which they should be executed.
     *
     * @return The new scripts.
     */
    protected SortedSet<Script> getNewScripts() {
        Map<String, Script> alreadyExecutedScriptMap = getScriptNameAlreadExecutedScriptMap();

        SortedSet<Script> result = new TreeSet<Script>();

        SortedSet<Script> allScripts = getAllUpdateScripts();
        for (Script script : allScripts) {
            Script alreadyExecutedScript = alreadyExecutedScriptMap.get(script.getFileName());

            // If the script is indexed and the version is higher than the highest one currently applied to the database,
            // add it to the list.
            if (script.isIncremental() && script.getVersion().compareTo(getHighestExecutedScriptVersion()) > 0) {
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


    protected Map<String, Script> getScriptNameAlreadExecutedScriptMap() {
        Map<String, Script> scriptNameAlreadExecutedScriptMap = new HashMap<String, Script>();
        for (ExecutedScript executedScript : executedScriptInfoSource.getExecutedScripts()) {
            scriptNameAlreadExecutedScriptMap.put(executedScript.getScript().getFileName(), executedScript.getScript());
        }
        return scriptNameAlreadExecutedScriptMap;
    }


    /**
     * @return a list of all available update scripts, in the order in which they must be executed on the database.
     * These scripts can be used to completely recreate the database from scratch. Not null
     */
    protected SortedSet<Script> getAllUpdateScripts() {
        SortedSet<Script> updateScripts = new TreeSet<Script>();
        for (Script script : scriptContainer.getScripts()) {
            if (!script.isPostProcessingScript()) {
                updateScripts.add(script);
            }
        }
        return updateScripts;
    }


    /**
     * @return All scripts that are incremental, i.e. non-repeatable, i.e. whose file name starts with an index
     */
    protected List<Script> getIncrementalScripts() {
        SortedSet<Script> scripts = getAllUpdateScripts();
        List<Script> incrementalScripts = new ArrayList<Script>();
        for (Script script : scripts) {
            if (script.isIncremental()) {
                incrementalScripts.add(script);
            }
        }
        return incrementalScripts;
    }


    //todo javadoc
    protected ScriptIndexes getHighestExecutedScriptVersion() {
        ScriptIndexes highest = new ScriptIndexes("0");
        for (ExecutedScript executedScript : executedScriptInfoSource.getExecutedScripts()) {
            Script script = executedScript.getScript();
            if (script.isIncremental() && script.getVersion().compareTo(highest) > 0) {
                highest = executedScript.getScript().getVersion();
            }
        }
        return highest;
    }


    /**
     * This operation updates the state of the database to indicate that all scripts have been executed, without actually
     * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
     * fixed a problem directly on the database.
     */
    public void markDatabaseAsUpToDate() {
        executedScriptInfoSource.clearAllExecutedScripts();

        SortedSet<Script> allScripts = getAllUpdateScripts();
        for (Script script : allScripts) {
            executedScriptInfoSource.registerExecutedScript(new ExecutedScript(script, new Date(), true));
        }
    }


    /**
     * This operation removes all database objects from the database, such as tables, views, sequences, synonyms and triggers.
     * The database schemas will be left untouched: this way, you can immediately start an update afterwards. This operation
     * is also called when a from-scratch update is performed. The table dbmaintain_scripts is not dropped but all data in
     * it is removed. It's possible to exclude certain database objects to make sure they are not dropped, like described
     * in {@link org.dbmaintain.clear.DBClearer}
     */
    public void clearDatabase() {
        // constraints are removed before clearing the database, to be sure there will be no
        // conflicts when dropping tables
        constraintsDisabler.disableConstraints();
        dbClearer.clearDatabase();
        executedScriptInfoSource.clearAllExecutedScripts();
    }


    /**
     * Updates the state of the database using the given scripts.
     *
     * @param scripts The scripts, not null
     */
    protected void updateDatabase(SortedSet<Script> scripts) {
        if (scripts.isEmpty()) {
            // nothing to do
            logger.info("Database is up to date");
            return;
        }
        logger.info("Database update scripts have been found and will be executed on the database.");

        // Remove data from the database, that could cause errors when executing scripts. Such
        // as for example when added a not null column.
        if (cleanDbEnabled) {
            dbCleaner.cleanDatabase();
        }

        // Execute all of the scripts
        executeScripts(scripts);

        // Execute postprocessing scripts, if any
        executePostProcessingScripts(scriptSource.getPostProcessingScripts());

        // Disable FK and not null constraints, if enabled
        if (disableConstraintsEnabled) {
            constraintsDisabler.disableConstraints();
        }
        // Update sequences to a sufficiently high value, if enabled
        if (updateSequencesEnabled) {
            sequenceUpdater.updateSequences();
        }
    }


    /**
     * Executes the given scripts and updates the database execution registry appropriately. After
     * each successful script execution, the script execution is registered in the database and marked
     * as successful. If a script execution fails, the script execution is registered in the database
     * and marked as unsuccessful.
     *
     * @param scripts The scripts to execute, not null
     */
    protected void executeScripts(SortedSet<Script> scripts) {
        for (Script script : scripts) {
            logger.info("Executing script " + script.getFileName());

            executeScript(script);
        }
    }


    /**
     * Executes the given script and updates the database execution registry appropriately. If
     * successfully, the script execution is registered in the database and marked as successful.
     * If an error occurred executing the script, the script execution is registered in the database
     * and marked as unsuccessful.
     *
     * @param script The script to execute, not null
     */
    protected void executeScript(Script script) {
        try {
            // We register the script execution, but we indicate it to be unsuccessful. If anything goes wrong or if the update is
            // interrupted before being completed, this will be the final state and the DbMaintainer will do a from-scratch update the next time
            ExecutedScript executedScript = new ExecutedScript(script, new Date(), false);
            executedScriptInfoSource.registerExecutedScript(executedScript);

            scriptRunner.execute(script);
            // We now register the previously registered script execution as being successful
            executedScript.setSuccessful(true);
            executedScriptInfoSource.updateExecutedScript(executedScript);

        } catch (DbMaintainException e) {
            logger.error("Error while executing script " + script.getFileName(), e);
            throw e;
        }
    }


    /**
     * Executes the given post processing scripts on the database. If not successful, the scripts update
     * is registered as not successful, so that an update from scratch will be triggered the next time.
     *
     * @param postProcessingScripts The scripts to execute, not null
     */
    protected void executePostProcessingScripts(SortedSet<Script> postProcessingScripts) {
        for (Script postProcessingScript : postProcessingScripts) {
            try {
                logger.info("Executing post processing script " + postProcessingScript.getFileName());

                scriptRunner.execute(postProcessingScript);

            } catch (DbMaintainException e) {
                logger.error("Error while executing post processing script " + postProcessingScript.getFileName(), e);
                throw e;
            }
        }
    }


    /**
     * Checks whether the database should be updated from scratch or just incrementally. The
     * database needs to be rebuilt in following cases:
     * <ul>
     * <li>Some existing scripts were modified.</li>
     * <li>The last update of the database was unsuccessful.</li>
     * </ul>
     * The database will only be rebuilt from scratch if from scratch is enabled. If the keep retrying is set to false,
     * the database will only be rebuilt again after an unsuccessful build when a change is made to the script files.
     * Otherwise it will not attempt to rebuild the database.
     *
     * @return True if a from scratch rebuild is needed, false otherwise
     */
    protected boolean shouldUpdateDatabaseFromScratch() {
        // check whether an existing script was updated
        if (isIncrementalScriptModified(executedScriptInfoSource.getExecutedScripts())) {
            if (!fromScratchEnabled) {
                throw new DbMaintainException("One or more existing incremental database update scripts have been modified, but updating from scratch is disabled. " +
                        "You should either revert to the original version of the modified script and add an new incremental script that performs the desired " +
                        "update, or perform the update manually on the database and then reset the database state by invoking resetDatabaseState()");
            }
            logger.info("One or more existing database update scripts have been modified. Database will be cleared and rebuilt from scratch.");
            return true;
        }

        // check whether the last run was successful
        if (errorInIndexedScriptDuringLastUpdate(executedScriptInfoSource.getExecutedScripts())) {
            if (fromScratchEnabled) {
                if (!keepRetryingAfterError) {
                    throw new DbMaintainException("During a previous database update, the execution of an incremental script failed! Since " +
                            PROPERTY_KEEP_RETRYING_AFTER_ERROR_ENABLED + " is set to false, the database will not be rebuilt " +
                            "from scratch, unless the failed (or another) incremental script is modified.");
                }
                logger.info("During a previous database update, the execution of a incremental script failed! " +
                        "Database will be cleared and rebuilt from scratch.");
                return true;
            } else {
                logger.warn("During a previous database update, the execution of an incremental script failed! " +
                        "Since from scratch updates are disabled, you should fix the erroneous script, solve the problem " +
                        "manually on the database, and then reset the database state by invoking resetDatabaseState()");
                return false;
            }
        }

        // from scratch is not needed
        return false;
    }


    /**
     * Returns true if one or more scripts that have a version index equal to or lower than
     * the index specified by the given version object has been modified since the timestamp specified by
     * the given version.
     *
     * @param currentVersion The current database version, not null
     * @return True if an existing script has been modified, false otherwise
     */
    protected boolean isIncrementalScriptModified(Set<ExecutedScript> alreadyExecutedScripts) {
        Map<String, Script> alreadyExecutedScriptMap = getScriptNameAlreadExecutedScriptMap();
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
            if (indexedScript.getVersion().compareTo(getHighestExecutedScriptVersion()) <= 0) {
                Script alreadyExecutedScript = alreadyExecutedScriptMap.get(indexedScript.getFileName());
                if (alreadyExecutedScript == null) {
                    if (indexedScript.isPatchScript()) {
                        if (!allowOutOfSequenceExecutionOfPatchScripts) {
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


    //todo javadoc
    protected boolean errorInIndexedScriptDuringLastUpdate(Set<ExecutedScript> alreadyExecutedScripts) {
        for (ExecutedScript script : alreadyExecutedScripts) {
            if (!script.isSucceeded() && script.getScript().isIncremental()) {
                return true;
            }
        }
        return false;
    }

}
