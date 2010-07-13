package org.dbmaintain;

import org.dbmaintain.clean.DbCleaner;
import org.dbmaintain.clear.DbClearer;
import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.config.MainFactory;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.format.ScriptUpdatesFormatter;
import org.dbmaintain.script.impl.ScriptRepository;
import org.dbmaintain.scriptrunner.ScriptRunner;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;

import static org.dbmaintain.config.DbMaintainProperties.*;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DbMaintainerFactory extends FactoryWithDatabase<DbMaintainer> {


    public DbMaintainer createInstance() {
        ScriptRepository scriptRepository = factoryWithDatabaseContext.createScriptRepository();

        boolean cleanDbEnabled = PropertyUtils.getBoolean(PROPERTY_CLEANDB, getConfiguration());
        boolean fromScratchEnabled = PropertyUtils.getBoolean(PROPERTY_FROM_SCRATCH_ENABLED, getConfiguration());
        boolean useScriptFileLastModificationDates = PropertyUtils.getBoolean(PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES, getConfiguration());
        boolean allowOutOfSequenceExecutionOfPatchScripts = PropertyUtils.getBoolean(PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, getConfiguration());
        boolean disableConstraintsEnabled = PropertyUtils.getBoolean(PROPERTY_DISABLE_CONSTRAINTS, getConfiguration());
        boolean updateSequencesEnabled = PropertyUtils.getBoolean(PROPERTY_UPDATE_SEQUENCES, getConfiguration());
        long maxNrOfCharsWhenLoggingScriptContent = PropertyUtils.getLong(PROPERTY_MAX_NR_CHARS_WHEN_LOGGING_SCRIPT_CONTENT, getConfiguration());
        ScriptIndexes baseLineRevision = factoryWithDatabaseContext.getBaselineRevision();

        MainFactory mainFactory = factoryWithDatabaseContext.getMainFactory();
        DbCleaner dbCleaner = mainFactory.createDbCleaner();
        DbClearer dbClearer = mainFactory.createDbClearer();
        ConstraintsDisabler constraintsDisabler = mainFactory.createConstraintsDisabler();
        SequenceUpdater sequenceUpdater = mainFactory.createSequenceUpdater();
        ScriptRunner scriptRunner = mainFactory.createScriptRunner();
        ScriptUpdatesFormatter scriptUpdatesFormatter = createScriptUpdatesFormatter();
        ExecutedScriptInfoSource executedScriptInfoSource = mainFactory.createExecutedScriptInfoSource();


        return new DefaultDbMaintainer(scriptRunner, scriptRepository, executedScriptInfoSource, fromScratchEnabled, useScriptFileLastModificationDates,
                allowOutOfSequenceExecutionOfPatchScripts, cleanDbEnabled, disableConstraintsEnabled, updateSequencesEnabled, dbClearer, dbCleaner,
                constraintsDisabler, sequenceUpdater, scriptUpdatesFormatter, getSqlHandler(), maxNrOfCharsWhenLoggingScriptContent, baseLineRevision);
    }


    protected ScriptUpdatesFormatter createScriptUpdatesFormatter() {
        return new ScriptUpdatesFormatter();
    }

}
