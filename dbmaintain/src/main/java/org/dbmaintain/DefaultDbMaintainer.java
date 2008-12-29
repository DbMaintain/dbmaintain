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
import org.apache.commons.lang.StringUtils;
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.script.*;
import org.dbmaintain.script.impl.ScriptRepository;
import static org.dbmaintain.script.ScriptUpdateType.*;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.util.DbMaintainException;

import java.util.*;

/**
 * Class that offers operations for automatically maintaining a database.
 * <p/>
 * The {@link #updateDatabase()} operation can be used to bring the database to the latest version. The
 * {@link #markDatabaseAsUpToDate()} operation updates the state of the database to indicate that all scripts have been
 * executed, without actually executing them. {@link DbMaintainer#clearDatabase()} will drop all tables and update the state to
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


    protected ScriptRepository scriptRepository;

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


    protected boolean cleanDbEnabled;

    /**
     * Indicates whether updating the database from scratch is enabled. If true, the database is
     * cleared before updating if an already executed script is modified
     */
    protected boolean fromScratchEnabled;


    protected boolean hasItemsToPreserve;

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
     * @param scriptRepository
     * @param executedScriptInfoSource Provides information about which scripts were already executed on the database
     * @param fromScratchEnabled If true, the database will be cleared and recreated from scratch if needed
     * @param hasItemsToPreserve
     * @param useScriptFileLastModificationDates
     * @param allowOutOfSequenceExecutionOfPatchScripts
     * @param allowOutOfSequenceExecutionOfPatchScripts
     * @param cleanDbEnabled If true, the data from all tables will be removed before performing any updates
     * @param disableConstraintsEnabled If true, all foreign key and not null constraints will be automatically disabled
     * or removed after each update
     * @param updateSequencesEnabled If true, the value of all sequences will be set to a minimal value after each update
     * @param dbClearer Helper object that can clear the database, i.e. drop all database objects
     * @param dbCleaner Helper object that can clean the database, i.e. remove the data from all tables
     * @param constraintsDisabler Helper object that can disable or remove all foreign key or not null constraints
     * @param sequenceUpdater Helper object that can update all sequences to a minimal value
     */
    public DefaultDbMaintainer(ScriptRunner scriptRunner, ScriptRepository scriptRepository, ExecutedScriptInfoSource executedScriptInfoSource,
               boolean fromScratchEnabled, boolean hasItemsToPreserve, boolean useScriptFileLastModificationDates, boolean allowOutOfSequenceExecutionOfPatchScripts,
               boolean cleanDbEnabled, boolean disableConstraintsEnabled, boolean updateSequencesEnabled, DBClearer dbClearer,
               DBCleaner dbCleaner, ConstraintsDisabler constraintsDisabler, SequenceUpdater sequenceUpdater) {

        this.scriptRunner = scriptRunner;
        this.scriptRepository = scriptRepository;
        this.executedScriptInfoSource = executedScriptInfoSource;
        this.fromScratchEnabled = fromScratchEnabled;
        this.hasItemsToPreserve = hasItemsToPreserve;
        this.useScriptFileLastModificationDates = useScriptFileLastModificationDates;
        this.allowOutOfSequenceExecutionOfPatchScripts = allowOutOfSequenceExecutionOfPatchScripts;
        this.cleanDbEnabled = cleanDbEnabled;
        this.disableConstraintsEnabled = disableConstraintsEnabled;
        this.updateSequencesEnabled = updateSequencesEnabled;
        this.dbClearer = dbClearer;
        this.dbCleaner = dbCleaner;
        this.constraintsDisabler = constraintsDisabler;
        this.sequenceUpdater = sequenceUpdater;
    }


    /**
     * This operation can be used to bring the database to the latest version. First it checks which scripts were already
     * applied to the database and executes the new scripts or the updated repeatable scripts. If an existing incremental
     * script was changed,  removed, or if a new incremental script has been added with a lower index than one that was
     * already executed, an error is given; unless the <fromScratch> option is enabled: in that case all database objects
     * at the end.
     */
    public void updateDatabase() {
        ScriptUpdates scriptUpdates = new ScriptUpdatesAnalyzer(scriptRepository, executedScriptInfoSource,
                useScriptFileLastModificationDates, allowOutOfSequenceExecutionOfPatchScripts).calculateScriptUpdates();
        
        if (scriptUpdates.isEmpty()) {
            if (!getScriptsThatFailedDuringLastUpdate().isEmpty()) {
                logger.error("During the last update, the execution of a script failed " + getScriptsThatFailedDuringLastUpdate().first());
            } else {
                logger.info("Database is up to date");
            }
            // Interrupt execution to make sure nothing is performed on the database
            return;
        }

        boolean recreateFromScratch = false;
        if (fromScratchEnabled && !hasItemsToPreserve && isInitialDatabaseUpdate()) {
            logger.info("Since the database is updated for the first time, the database is cleared first to be sure we start with a clean database");
            recreateFromScratch = true;
        }
        
        if (scriptUpdates.hasIrregularScriptUpdates()) {
            if (fromScratchEnabled) {
                // Recreate the database from scratch
                logger.info("The database is recreated from scratch, because one or more irregular script updates were detected:\n" +
                        formatIrregularUpdates(scriptUpdates));
                recreateFromScratch = true;
            } else {
                throw new DbMaintainException("Irregular script updates detected, but fromScratch updates are disabled. To solve this problem, you can do one of the following:\n" +
                    "  1: Revert the changes and perform the desired changes using incremental scripts\n" +
                    "  2: Enable the fromScratch option so that the database is recreated from scratch (all data will be lost)\n" +
                    "  3: Fix the database manually and invoke the markDatabaseAsUpToDate operation (error prone)\n\n" +
                    "Following irregular updates were performed:\n" + formatIrregularUpdates(scriptUpdates) + "\n");
            }
        }

        if (recreateFromScratch) {
            // Clear the database and execute all scripts from-scratch
            clearDatabase();
            executeScripts(scriptRepository.getAllUpdateScripts());
        } else {
            logger.info("The database is updated incrementally, since following regular script updates were detected:\n" + formatRegularUpdates(scriptUpdates));

            // If cleandb is enabled, remove all data from the database.
            if (cleanDbEnabled) {
                dbCleaner.cleanDatabase();
            }
            // If there are incremental patch scripts with a lower index and the option allowOutOfSequenceExecutionOfPatches
            // is enabled, execute them first
            executeScriptUpdates(scriptUpdates.getRegularPatchScriptUpdates());
            // Execute all new incremental and all new or modified repeatable scripts
            executeScriptUpdates(scriptUpdates.getRegularScriptUpdates());
        }
        // Execute all post processing scripts
        executeScripts(scriptRepository.getPostProcessingScripts());

        // If the disable constraints option is enabled, disable all FK and not null constraints
        if (disableConstraintsEnabled) {
            constraintsDisabler.disableConstraints();
        }
        // If the update sequences option is enabled, update all sequences to have a value equal to or higher than the configured threshold
        if (updateSequencesEnabled) {
            sequenceUpdater.updateSequences();
        }
    }


    /**
     * @return Whether we are running dbmaintain for the first time. If there are no scripts available yet, this method
     * returns false.
     */
    protected boolean isInitialDatabaseUpdate() {
        return executedScriptInfoSource.getExecutedScripts().size() == 0 && scriptRepository.areScriptsAvailable();
    }


    /**
     * @param scriptUpdates The script updates, not null
     * @return An printable overview of the regular script updates
     */
    protected String formatRegularUpdates(ScriptUpdates scriptUpdates) {
        StringBuilder formattedUpdates = new StringBuilder();
        int index = 0;
        for (ScriptUpdate scriptUpdate : scriptUpdates.getRegularScriptUpdates(HIGHER_INDEX_SCRIPT_ADDED)) {
            formattedUpdates.append("  ").append(++index).append(". ").append(StringUtils.capitalize(formatScriptUpdate(scriptUpdate))).append("\n");
        }
        for (ScriptUpdate scriptUpdate : scriptUpdates.getRegularScriptUpdates(REPEATABLE_SCRIPT_ADDED)) {
            formattedUpdates.append("  ").append(++index).append(". ").append(StringUtils.capitalize(formatScriptUpdate(scriptUpdate))).append("\n");
        }
        for (ScriptUpdate scriptUpdate : scriptUpdates.getRegularScriptUpdates(REPEATABLE_SCRIPT_UPDATED)) {
            formattedUpdates.append("  ").append(++index).append(". ").append(StringUtils.capitalize(formatScriptUpdate(scriptUpdate))).append("\n");
        }
        return formattedUpdates.toString();
    }


    /**
     * @param scriptUpdates The script updates, not null
     * @return An printable overview of the irregular script updates
     */
    protected String formatIrregularUpdates(ScriptUpdates scriptUpdates) {
        StringBuilder formattedUpdates = new StringBuilder();
        int index = 0;
        for (ScriptUpdate scriptUpdate : scriptUpdates.getIrregularScriptUpdates(INDEXED_SCRIPT_UPDATED)) {
            formattedUpdates.append("  ").append(++index).append(". ").append(StringUtils.capitalize(formatScriptUpdate(scriptUpdate))).append("\n");
        }
        for (ScriptUpdate scriptUpdate : scriptUpdates.getIrregularScriptUpdates(INDEXED_SCRIPT_DELETED)) {
            formattedUpdates.append("  ").append(++index).append(". ").append(StringUtils.capitalize(formatScriptUpdate(scriptUpdate))).append("\n");
        }
        for (ScriptUpdate scriptUpdate : scriptUpdates.getIrregularScriptUpdates(LOWER_INDEX_NON_PATCH_SCRIPT_ADDED)) {
            formattedUpdates.append("  ").append(++index).append(". ").append(StringUtils.capitalize(formatScriptUpdate(scriptUpdate))).append("\n");
        }
        for (ScriptUpdate scriptUpdate : scriptUpdates.getIrregularScriptUpdates(LOWER_INDEX_PATCH_SCRIPT_ADDED)) {
            formattedUpdates.append("  ").append(++index).append(". ").append(StringUtils.capitalize(formatScriptUpdate(scriptUpdate))).append("\n");
        }
        return formattedUpdates.toString();
    }


    /**
     * @param scriptUpdate The script update to format, not null
     * @return A printable view of the given script update
     */
    protected String formatScriptUpdate(ScriptUpdate scriptUpdate) {
        switch (scriptUpdate.getType()) {
            case HIGHER_INDEX_SCRIPT_ADDED:
                return "newly added indexed script: " + scriptUpdate.getScript().getFileName();
            case REPEATABLE_SCRIPT_ADDED:
                return "newly added repeatable script: " + scriptUpdate.getScript().getFileName();
            case REPEATABLE_SCRIPT_UPDATED:
                return "updated repeatable script: " + scriptUpdate.getScript().getFileName();
            case REPEATABLE_SCRIPT_DELETED:
                return "deleted repeatable script: " + scriptUpdate.getScript().getFileName();
            case POSTPROCESSING_SCRIPT_ADDED:
                return "newly added postprocessing script: " + scriptUpdate.getScript().getFileName();
            case POSTPROCESSING_SCRIPT_UPDATED:
                return "updated postprocessing script: " + scriptUpdate.getScript().getFileName();
            case POSTPROCESSING_SCRIPT_DELETED:
                return "deleted postprocessing script: " + scriptUpdate.getScript().getFileName();
            case INDEXED_SCRIPT_UPDATED:
                return "updated indexed script: " + scriptUpdate.getScript().getFileName();
            case INDEXED_SCRIPT_DELETED:
                return "deleted indexed script: " + scriptUpdate.getScript().getFileName();
            case LOWER_INDEX_NON_PATCH_SCRIPT_ADDED:
                return "newly added script with a lower index: "
                    + scriptUpdate.getScript().getFileName();
            case LOWER_INDEX_PATCH_SCRIPT_ADDED:
                return "newly added patch script with a lower index, with out-of-sequence execution of patch scripts disabled: "
                    + scriptUpdate.getScript().getFileName();
        }
        throw new IllegalArgumentException("Invalid script update type " + scriptUpdate.getType());
    }


    /**
     * @return The already executed scripts, as a map from Script => ExecutedScript
     */
    protected Map<Script, ExecutedScript> getAlreadyExecutedScripts() {
        Map<Script, ExecutedScript> alreadyExecutedScripts = new HashMap<Script, ExecutedScript>();
        for (ExecutedScript executedScript : executedScriptInfoSource.getExecutedScripts()) {
            alreadyExecutedScripts.put(executedScript.getScript(), executedScript);
        }
        return alreadyExecutedScripts;
    }


    /**
     * This operation updates the state of the database to indicate that all scripts have been executed, without actually
     * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
     * fixed a problem directly on the database.
     */
    public void markDatabaseAsUpToDate() {
        executedScriptInfoSource.clearAllExecutedScripts();

        SortedSet<Script> allScripts = scriptRepository.getAllScripts();
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
     *
     */
    public void clearDatabase() {
        // Constraints are removed before clearing the database, to be sure there will be no conflicts when dropping tables
        constraintsDisabler.disableConstraints();
        dbClearer.clearDatabase();
        executedScriptInfoSource.clearAllExecutedScripts();
    }


    /**
     * Executes the given scripts and updates the database execution registry appropriately. After
     * each successful script execution, the script execution is registered in the database and marked
     * as successful. If a script execution fails, the script execution is registered in the database
     * and marked as unsuccessful.
     *
     * @param scriptUpdates
     */
    protected void executeScriptUpdates(SortedSet<ScriptUpdate> scriptUpdates) {
        for (ScriptUpdate scriptUpdate : scriptUpdates) {
            logger.info("Executing " + formatScriptUpdate(scriptUpdate));
            executeScript(scriptUpdate.getScript());
        }
    }


    /**
     * Executes the given scripts and updates the database execution registry appropriately. After
     * each successful script execution, the script execution is registered in the database and marked
     * as successful. If a script execution fails, the script execution is registered in the database
     * and marked as unsuccessful.
     *
     * @param scripts
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
            throw new DbMaintainException("Error while executing script " + script.getFileName() + ": " + e.getMessage(), e);
        }
    }


    /*protected boolean shouldUpdateDatabaseFromScratch() {
        // check whether an existing script was updated
        // check whether the last run was successful
        if (errorInIndexedScriptDuringLastUpdate()) {
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
    } */


    protected SortedSet<ExecutedScript> getScriptsThatFailedDuringLastUpdate() {
        SortedSet<ExecutedScript> failedExecutedScripts = new TreeSet<ExecutedScript>();
        for (ExecutedScript script : executedScriptInfoSource.getExecutedScripts()) {
            if (!script.isSucceeded()) {
                failedExecutedScripts.add(script);
            }
        }
        return failedExecutedScripts;
    }


    protected boolean errorInIndexedScriptDuringLastUpdate() {
        for (ExecutedScript script : executedScriptInfoSource.getExecutedScripts()) {
            if (script.getScript().isIncremental() && !script.isSucceeded()) {
                return true;
            }
        }
        return false;
    }

}
