package org.dbmaintain;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.script.analyzer.ScriptUpdatesFormatter;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.repository.ScriptRepository;
import org.dbmaintain.script.runner.ScriptRunner;
import org.dbmaintain.structure.clean.DBCleaner;
import org.dbmaintain.structure.clear.DBClearer;
import org.dbmaintain.structure.constraint.ConstraintsDisabler;
import org.dbmaintain.structure.sequence.SequenceUpdater;

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
        DBCleaner dbCleaner = mainFactory.createDBCleaner();
        DBClearer dbClearer = mainFactory.createDBClearer();
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
