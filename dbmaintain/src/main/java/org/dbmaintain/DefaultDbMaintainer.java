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
import org.dbmaintain.clean.DBClearer;
import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptRunner;
import org.dbmaintain.script.ScriptSource;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.version.ExecutedScriptInfoSource;
import org.dbmaintain.version.ScriptIndexes;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * A class for performing automatic maintenance of a database.<br>
 * This class must be configured with implementations of a {@link ExecutedScriptInfoSource},
 * {@link ScriptSource}, a {@link ScriptRunner}, {@link DBClearer}, {@link DBCleaner},
 * {@link ConstraintsDisabler} and a {@link SequenceUpdater}.
 * <p/> The {@link #updateDatabase()} method check what is the current version of the database, and
 * see if existing scripts have been modified. If yes, the database is cleared and all available
 * database scripts, are executed on the database. If no existing scripts have been modified, but
 * new scripts were added, only the new scripts are executed. Before executing an update, data from
 * the database is removed, to avoid problems when e.g. adding a not null column. <p/> If a database
 * update causes an error, a {@link DbMaintainException} is thrown. After a failing update, the
 * database is always completely recreated from scratch. <p/> After updating the database, following
 * steps are optionally executed on the database (depending on the configuration):
 * <ul>
 * <li>Foreign key and not null constraints are disabled.</li>
 * <li>Sequences and identity columns that have a value lower than a configured treshold, are
 * updated to a value equal to or larger than this treshold</li>
 * <li>A DTD is generated that describes the database's table structure, to use in test data XML
 * files</li>
 * </ul>
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
     * Indicates if foreign key and not null constraints should removed after updating the database
     * structure
     */
    protected boolean disableConstraintsEnabled;

    /**
     * Indicates whether sequences and identity columns must be updated to a certain minimal value
     */
    protected boolean updateSequencesEnabled;


    /**
     * Default constructor for testing.
     */
    protected DefaultDbMaintainer() {
    }


    //todo javadoc
    public DefaultDbMaintainer(ScriptRunner scriptRunner, ScriptSource scriptSource, ExecutedScriptInfoSource executedScriptInfoSource,
                               boolean fromScratchEnabled, boolean cleanDbEnabled, boolean disableConstraintsEnabled, boolean updateSequencesEnabled,
                               DBClearer dbClearer, DBCleaner dbCleaner, ConstraintsDisabler constraintsDisabler, SequenceUpdater sequenceUpdater) {
        this.scriptRunner = scriptRunner;
        this.scriptSource = scriptSource;
        this.executedScriptInfoSource = executedScriptInfoSource;
        this.fromScratchEnabled = fromScratchEnabled;
        this.cleanDbEnabled = cleanDbEnabled;
        this.disableConstraintsEnabled = disableConstraintsEnabled;
        this.updateSequencesEnabled = updateSequencesEnabled;
        this.dbClearer = dbClearer;
        this.dbCleaner = dbCleaner;
        this.constraintsDisabler = constraintsDisabler;
        this.sequenceUpdater = sequenceUpdater;
    }


    //todo javadoc
    public void updateDatabase() {
        // Check if the executed scripts info source recommends a from-scratch update
        boolean fromScratchUpdateRecommended = executedScriptInfoSource.isFromScratchUpdateRecommended();
        
        Set<ExecutedScript> alreadyExecutedScripts = executedScriptInfoSource.getExecutedScripts();
        ScriptIndexes highestExecutedScriptVersion = getHighestExecutedScriptVersion(alreadyExecutedScripts);

        // check whether an incremental update can be performed
        if (!(fromScratchUpdateRecommended && fromScratchEnabled) && !shouldUpdateDatabaseFromScratch(highestExecutedScriptVersion, alreadyExecutedScripts)) {
            // update database with new scripts
            updateDatabase(scriptSource.getNewScripts(highestExecutedScriptVersion, alreadyExecutedScripts));
            return;
        }

        // From scratch needed, clear the database and retrieve scripts
        // constraints are removed before clearing the database, to be sure there will be no
        // conflicts when dropping tables
        constraintsDisabler.disableConstraints();
        dbClearer.clearDatabase();
        // reset the database version
        executedScriptInfoSource.clearAllExecutedScripts();
        // update database with all scripts
        updateDatabase(scriptSource.getAllUpdateScripts());
    }


    //todo javadoc
    protected ScriptIndexes getHighestExecutedScriptVersion(Set<ExecutedScript> executedScripts) {
        ScriptIndexes highest = new ScriptIndexes("0");
        for (ExecutedScript executedScript : executedScripts) {
        	Script script = executedScript.getScript();
            if (script.isIncremental() && script.getVersion().compareTo(highest) > 0) {
            	highest = executedScript.getScript().getVersion();
            }
        }
        return highest;
    }


    //todo javadoc
    public void markDatabaseAsUptodate() {
        executedScriptInfoSource.clearAllExecutedScripts();

        List<Script> allScripts = scriptSource.getAllUpdateScripts();
        for (Script script : allScripts) {
            executedScriptInfoSource.registerExecutedScript(new ExecutedScript(script, new Date(), true));
        }
    }


    public void clearDatabase() {
        dbClearer.clearDatabase();
        executedScriptInfoSource.clearAllExecutedScripts();
    }


    /**
     * Updates the state of the database using the given scripts.
     *
     * @param scripts The scripts, not null
     */
    protected void updateDatabase(List<Script> scripts) {
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
    protected void executeScripts(List<Script> scripts) {
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
    protected void executePostProcessingScripts(List<Script> postProcessingScripts) {
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
     * todo check javadoc
     * <p/>
     * Checks whether the database should be updated from scratch or just incrementally. The
     * database needs to be rebuilt in following cases:
     * <ul>
     * <li>Some existing scripts were modified.</li>
     * <li>The last update of the database was unsuccessful.</li>
     * </ul>
     *
     * @param currentVersion         The current database version, not null
     * @param alreadyExecutedScripts The current set of executed scripts, not null
     * @return True if a from scratch rebuild is needed, false otherwise
     */
    protected boolean shouldUpdateDatabaseFromScratch(ScriptIndexes currentVersion, Set<ExecutedScript> alreadyExecutedScripts) {
        // check whether the last run was successful
        /*if (errorInIndexedScriptDuringLastUpdate(alreadyExecutedScripts)) {
        	if (fromScratchEnabled) {
        		if (!keepRetryingAfterError) {
                    logger.warn("During a previous database update, the execution of an incremental script failed! Since " + 
                		PROPKEY_KEEP_RETRYING_AFTER_ERROR_ENABLED + " is set to false, the database will not be rebuilt " +
        				"from scratch, unless the failed (or another) incremental script is modified.");
                    return false;
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
        }*/


        // check whether an existing script was updated
        if (scriptSource.isIncrementalScriptModified(currentVersion, alreadyExecutedScripts)) {
            if (fromScratchEnabled) {
                logger.info("One or more existing database update scripts have been modified. Database will be cleared and rebuilt from scratch.");
                return true;

            }
            throw new DbMaintainException("One or more existing incremental database update scripts have been modified, but updating from scratch is disabled. " +
                    "You should revert to the original version of the modified script and add an new incremental script that performs the desired update");
        }

        // from scratch is not needed
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
