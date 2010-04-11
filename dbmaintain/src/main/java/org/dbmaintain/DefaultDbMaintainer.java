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
import org.dbmaintain.dbsupport.SQLHandler;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.format.ScriptUpdatesFormatter;
import org.dbmaintain.script.*;
import org.dbmaintain.script.impl.ScriptRepository;
import org.dbmaintain.scriptrunner.ScriptRunner;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.util.DbMaintainException;

import java.util.*;

import static org.dbmaintain.script.ScriptUpdateType.REPEATABLE_SCRIPT_DELETED;
import static org.dbmaintain.script.ScriptUpdateType.REPEATABLE_SCRIPT_UPDATED;

/**
 * Class that offers operations for automatically maintaining a database.
 * <p/>
 * The {@link #updateDatabase} operation can be used to bring the database to the latest version. The
 * {@link #markDatabaseAsUpToDate} operation updates the state of the database to indicate that all scripts have been
 * executed, without actually executing them. {@link DbMaintainer#clearDatabase} will drop all tables and update the state to
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

    /**
     * Handles all SQL statements
     */
    protected SQLHandler sqlHandler;

    protected boolean cleanDb;

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
    protected boolean disableConstraints;

    /**
     * Indicates whether sequences and identity columns must be updated to a certain minimal value
     */
    protected boolean updateSequences;

    /**
     * Formats the script updates in order to output it to the user
     */
    protected ScriptUpdatesFormatter scriptUpdatesFormatter;


    /**
     * Creates a new instance
     *
     * @param scriptRunner             runner that executes the database scripts
     * @param scriptRepository         provides access to all database scripts
     * @param executedScriptInfoSource provides information about which scripts were already executed on the database
     * @param fromScratchEnabled       if true, the database will be cleared and recreated from scratch if needed
     * @param hasItemsToPreserve       if true, there are items in the database that must be preserved when performing a from-scratch update
     * @param useScriptFileLastModificationDates
     *                                 if true, the dbmaintainer decides that a script hasn't changed if the
     *                                 last modification date is identical to the one of the last update, without looking at the contents of the script
     * @param allowOutOfSequenceExecutionOfPatchScripts
     *                                 if true, patch scripts can be executed out-of-sequence
     * @param cleanDb                  if true, the data from all tables is removed before performing any updates
     * @param disableConstraints       if true, all foreign key and not null constraints are automatically disabled
     *                                 or removed after each update
     * @param updateSequences          if true, the value of all sequences is set to a minimal value after each update
     * @param dbClearer                helper object that clears the database, i.e. drop all database objects
     * @param dbCleaner                helper object that cleans the database, i.e. remove the data from all tables
     * @param constraintsDisabler      helper object that disables or removes all foreign key or not null constraints
     * @param sequenceUpdater          helper object that updates all sequences to a minimal value
     * @param scriptUpdatesFormatter   helper object that formats the script updates in a well-readable format for the user
     * @param sqlHandler               helper object that performs sql statements on the database
     */
    public DefaultDbMaintainer(ScriptRunner scriptRunner, ScriptRepository scriptRepository, ExecutedScriptInfoSource executedScriptInfoSource,
                               boolean fromScratchEnabled, boolean hasItemsToPreserve, boolean useScriptFileLastModificationDates, boolean allowOutOfSequenceExecutionOfPatchScripts,
                               boolean cleanDb, boolean disableConstraints, boolean updateSequences, DBClearer dbClearer, DBCleaner dbCleaner, ConstraintsDisabler constraintsDisabler,
                               SequenceUpdater sequenceUpdater, ScriptUpdatesFormatter scriptUpdatesFormatter, SQLHandler sqlHandler) {

        this.scriptRunner = scriptRunner;
        this.scriptRepository = scriptRepository;
        this.executedScriptInfoSource = executedScriptInfoSource;
        this.fromScratchEnabled = fromScratchEnabled;
        this.hasItemsToPreserve = hasItemsToPreserve;
        this.useScriptFileLastModificationDates = useScriptFileLastModificationDates;
        this.allowOutOfSequenceExecutionOfPatchScripts = allowOutOfSequenceExecutionOfPatchScripts;
        this.cleanDb = cleanDb;
        this.disableConstraints = disableConstraints;
        this.updateSequences = updateSequences;
        this.dbClearer = dbClearer;
        this.dbCleaner = dbCleaner;
        this.constraintsDisabler = constraintsDisabler;
        this.sequenceUpdater = sequenceUpdater;
        this.scriptUpdatesFormatter = scriptUpdatesFormatter;
        this.sqlHandler = sqlHandler;
    }


    /**
     * This operation can be used to bring the database to the latest version. First it checks which scripts were already
     * applied to the database and executes the new scripts or the updated repeatable scripts. If an existing incremental
     * script was changed,  removed, or if a new incremental script has been added with a lower index than one that was
     * already executed, an error is given; unless the <fromScratch> option is enabled: in that case all database objects
     * at the end.
     *
     * @param dryRun if true, no updates have to be performed on the database - we do a simulation of the database update
     *               instead of actually performing the database update.
     * @return whether updates were performed on the database
     */
    public boolean updateDatabase(boolean dryRun) {
        try {
            ScriptUpdates scriptUpdates = getScriptUpdates();

            if (!getIncrementalScriptsThatFailedDuringLastUpdate().isEmpty() && !scriptUpdates.hasIrregularScriptUpdates()) {
                throw new DbMaintainException("During the latest update, the execution of the following incremental script failed: " +
                        getIncrementalScriptsThatFailedDuringLastUpdate().first() + ". \nThis problem must be fixed before any other " +
                        "updates can be performed.");
            }

            if (!getRepeatableScriptsThatFailedDuringLastUpdate().isEmpty() && !scriptUpdates.hasIrregularScriptUpdates()) {
                ExecutedScript failedScript = getRepeatableScriptsThatFailedDuringLastUpdate().first();
                if (!scriptUpdates.getRegularlyAddedOrModifiedScripts().contains(new ScriptUpdate(REPEATABLE_SCRIPT_UPDATED, failedScript.getScript()))
                        && !scriptUpdates.getRegularlyDeletedRepeatableScripts().contains(new ScriptUpdate(REPEATABLE_SCRIPT_DELETED, failedScript.getScript()))) {
                    throw new DbMaintainException("During the latest update, the execution of the following repeatable script failed: " +
                            getRepeatableScriptsThatFailedDuringLastUpdate().first() + ". \nThe script that causes this problem must be fixed " +
                            "before any other updates can be performed: This can be the failed script itself or an incremental script.");
                }
            }

            if (scriptUpdates.isEmpty()) {
                logger.info("The database is up to date");
                return false;
            }

            boolean recreateFromScratch = false;
            if (fromScratchEnabled && !hasItemsToPreserve && isInitialDatabaseUpdate()) {
                logger.info("The database is updated for the first time. The database is cleared to be sure that we start with a clean database");
                recreateFromScratch = true;
            }

            if (scriptUpdates.hasIrregularScriptUpdates()) {
                if (fromScratchEnabled) {
                    // Recreate the database from scratch
                    logger.info("The database is recreated from scratch, since following irregular script updates were detected:\n" + scriptUpdatesFormatter.formatScriptUpdates(scriptUpdates.getIrregularScriptUpdates()));
                    recreateFromScratch = true;
                } else {
                    throw new DbMaintainException("Following irregular script updates were detected:\n" + scriptUpdatesFormatter.formatScriptUpdates(scriptUpdates.getIrregularScriptUpdates()) +
                            "\nBecause of this, dbmaintain can't perform the update. To solve this problem, you can do one of the following:\n" +
                            "  1: Revert the irregular updates and use regular script updates instead\n" +
                            "  2: Enable the fromScratch option so that the database is recreated from scratch (all data will be lost)\n" +
                            "  3: Perform the updates manually on the database and invoke the markDatabaseAsUpToDate operation (error prone)\n");
                }
            }

            if (recreateFromScratch) {
                logger.info("The database is cleared, and all database scripts are executed.");
                if (!dryRun) {
                    doClearDatabase();
                    executeScripts(scriptRepository.getAllUpdateScripts());
                }
            } else {
                logger.info("The database is updated incrementally, since following regular script updates were detected:\n" + scriptUpdatesFormatter.formatScriptUpdates(scriptUpdates.getRegularScriptUpdates()));
                if (!dryRun) {
                    // If cleandb is enabled, remove all data from the database.
                    if (cleanDb) {
                        dbCleaner.cleanDatabase();
                    }
                    // If there are incremental patch scripts with a lower index and the option allowOutOfSequenceExecutionOfPatches
                    // is enabled, execute them first
                    executeScriptUpdates(scriptUpdates.getRegularlyAddedPatchScripts());
                    // Execute all new incremental and all new or modified repeatable scripts
                    executeScriptUpdates(scriptUpdates.getRegularlyAddedOrModifiedScripts());
                    // If repeatable scripts were removed, also remove them from the executed scripts
                    removeDeletedRepeatableScriptsFromExecutedScripts(scriptUpdates.getRegularlyDeletedRepeatableScripts());
                    // If regular script renames were detected, update the executed script records to reflect this
                    performRegularScriptRenamesInExecutedScripts(scriptUpdates.getRegularlyRenamedScripts());
                }
            }
            if (scriptUpdates.noUpdatesOtherThanRepeatableScriptDeletionsOrRenames()) {
                logger.info("No script updates were detected, except for repeatable script deletions and script renames. Therefore, actions such as the execution of postprocessing scripts and disabling the constraints are skipped.");
                return false;
            }

            if (!dryRun) {
                // Execute all post processing scripts
                executePostprocessingScripts();

                // If the disable constraints option is enabled, disable all FK and not null constraints
                if (disableConstraints) {
                    constraintsDisabler.disableConstraints();
                }
                // If the update sequences option is enabled, update all sequences to have a value equal to or higher than the configured threshold
                if (updateSequences) {
                    sequenceUpdater.updateSequences();
                }
                logger.info("The database has been updated successfully.");
            }
            return true;

        } finally {
            sqlHandler.closeAllConnections();
        }
    }


    /**
     * This operation calcutes and logs which script updates have been performed since the last database update.
     *
     * @return the scripts that have been updated since the last database update
     */
    public ScriptUpdates getScriptUpdates() {
        return new ScriptUpdatesAnalyzer(scriptRepository, executedScriptInfoSource, useScriptFileLastModificationDates,
                allowOutOfSequenceExecutionOfPatchScripts).calculateScriptUpdates();
    }


    /**
     * @return Whether we are running dbmaintain for the first time. If there are no scripts available yet, this method
     *         returns false.
     */
    protected boolean isInitialDatabaseUpdate() {
        return executedScriptInfoSource.getExecutedScripts().size() == 0 && scriptRepository.areScriptsAvailable();
    }


    /**
     * Executes all postprocessing scripts
     */
    protected void executePostprocessingScripts() {
        executedScriptInfoSource.deleteAllExecutedPostprocessingScripts();
        executeScripts(scriptRepository.getPostProcessingScripts());
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
     * Removes all executed scripts that indicate repeatable scripts that were removed since the last database update
     *
     * @param repeatableScriptDeletions The scripts that were removed since the last database updates
     */
    protected void removeDeletedRepeatableScriptsFromExecutedScripts(SortedSet<ScriptUpdate> repeatableScriptDeletions) {
        for (ScriptUpdate deletedRepeatableScriptUpdate : repeatableScriptDeletions) {
            executedScriptInfoSource.deleteExecutedScript(getAlreadyExecutedScripts().get(deletedRepeatableScriptUpdate.getScript()));
        }
    }


    /**
     * Updates the records in the DBMAINTAIN_SCRIPTS table for all scripts that were regularly renamed (i.e. renamed
     * without changing the order of the incremental scripts.
     *
     * @param regularScriptRenames the scripts that were regularly renamed
     */
    protected void performRegularScriptRenamesInExecutedScripts(SortedSet<ScriptUpdate> regularScriptRenames) {
        for (ScriptUpdate regularScriptRename : regularScriptRenames) {
            executedScriptInfoSource.renameExecutedScript(getAlreadyExecutedScripts().get(regularScriptRename.getScript()), regularScriptRename.getRenamedToScript());
        }
    }


    /**
     * This operation updates the state of the database to indicate that all scripts have been executed, without actually
     * executing them. This can be useful when you want to start using DbMaintain on an existing database, or after having
     * fixed a problem directly on the database.
     */
    public void markDatabaseAsUpToDate() {
        try {
            executedScriptInfoSource.clearAllExecutedScripts();

            SortedSet<Script> allScripts = scriptRepository.getAllScripts();
            for (Script script : allScripts) {
                executedScriptInfoSource.registerExecutedScript(new ExecutedScript(script, new Date(), true));
            }
            logger.info("The database has been marked as up-to-date");
        } finally {
            sqlHandler.closeAllConnections();
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
        try {
            doClearDatabase();
        } finally {
            sqlHandler.closeAllConnections();
        }
    }


    /**
     * This operation removes all database objects from the database, such as tables, views, sequences, synonyms and triggers.
     * The database schemas will be left untouched: this way, you can immediately start an update afterwards. This operation
     * is also called when a from-scratch update is performed. The table dbmaintain_scripts is not dropped but all data in
     * it is removed. It's possible to exclude certain database objects to make sure they are not dropped, like described
     * in {@link org.dbmaintain.clear.DBClearer}
     */
    protected void doClearDatabase() {
        // Constraints are removed before clearing the database, to be sure there will be no conflicts when dropping tables
        constraintsDisabler.disableConstraints();
        dbClearer.clearDatabase();
        executedScriptInfoSource.clearAllExecutedScripts();
    }


    /**
     * This operation deletes all data from the database, except for the DBMAINTAIN_SCRIPTS table.
     */
    public void cleanDatabase() {
        try {
            dbCleaner.cleanDatabase();
        } finally {
            sqlHandler.closeAllConnections();
        }
    }


    /**
     * This operation disables all foreign key and not null constraints of the database schemas. Primary key constraints
     * are left untouched.
     */
    public void disableConstraints() {
        try {
            constraintsDisabler.disableConstraints();
        } finally {
            sqlHandler.closeAllConnections();
        }
    }


    /**
     * This operation thatupdates all sequences and identity columns to a minimum value.
     */
    public void updateSequences() {
        try {
            sequenceUpdater.updateSequences();
        } finally {
            sqlHandler.closeAllConnections();
        }
    }

    /**
     * Executes the given scripts and updates the database execution registry appropriately. After
     * each successful script execution, the script execution is registered in the database and marked
     * as successful. If a script execution fails, the script execution is registered in the database
     * and marked as unsuccessful.
     *
     * @param scriptUpdates the script updates to be executed
     */
    protected void executeScriptUpdates(SortedSet<ScriptUpdate> scriptUpdates) {
        scriptRunner.initialize();
        try {
            for (ScriptUpdate scriptUpdate : scriptUpdates) {
                logger.info("Executing " + scriptUpdatesFormatter.formatScriptUpdate(scriptUpdate));
                executeScript(scriptUpdate.getScript());
            }
        } finally {
            scriptRunner.close();
        }
    }


    /**
     * Executes the given scripts and updates the database execution registry appropriately. After
     * each successful script execution, the script execution is registered in the database and marked
     * as successful. If a script execution fails, the script execution is registered in the database
     * and marked as unsuccessful.
     *
     * @param scripts the scripts to be executed on the database
     */
    protected void executeScripts(SortedSet<Script> scripts) {
        scriptRunner.initialize();
        try {
            for (Script script : scripts) {
                logger.info("Executing script " + script.getFileName());
                executeScript(script);
            }
        } finally {
            scriptRunner.close();
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
            String scriptContents = script.getScriptContentHandle().getScriptContents();
            String message = "Error while executing script " + script.getFileName() + ":\n" + e.getMessage() + "\n\n";
            message += "Full contents of failed script " + script.getFileName() + ":\n";
            message += "----------------------------------------------------\n";
            message += scriptContents + "\n";
            message += "----------------------------------------------------\n";
            throw new DbMaintainException(message, e.getCause());
        }
    }


    /**
     * @return the incremental scripts that failed during the last database update
     */
    protected SortedSet<ExecutedScript> getIncrementalScriptsThatFailedDuringLastUpdate() {
        SortedSet<ExecutedScript> failedExecutedScripts = new TreeSet<ExecutedScript>();
        for (ExecutedScript script : executedScriptInfoSource.getExecutedScripts()) {
            if (!script.isSuccessful() && script.getScript().isIncremental()) {
                failedExecutedScripts.add(script);
            }
        }
        return failedExecutedScripts;
    }


    /**
     * @return the repeatable scripts that failed during the last database update
     */
    protected SortedSet<ExecutedScript> getRepeatableScriptsThatFailedDuringLastUpdate() {
        SortedSet<ExecutedScript> failedExecutedScripts = new TreeSet<ExecutedScript>();
        for (ExecutedScript script : executedScriptInfoSource.getExecutedScripts()) {
            if (!script.isSuccessful() && script.getScript().isRepeatable()) {
                failedExecutedScripts.add(script);
            }
        }
        return failedExecutedScripts;
    }

}
