package org.dbmaintain.script.executedscriptinfo;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.database.Database;
import org.dbmaintain.script.executedscriptinfo.impl.DefaultExecutedScriptInfoSource;
import org.dbmaintain.script.qualifier.Qualifier;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Set;

import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.config.PropertyUtils.getStringList;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class ExecutedScriptInfoSourceFactory extends FactoryWithDatabase<ExecutedScriptInfoSource> {


    public ExecutedScriptInfoSource createInstance() {
        boolean autoCreateExecutedScriptsTable = PropertyUtils.getBoolean(PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, getConfiguration());

        Database defaultDatabase = getDatabases().getDefaultDatabase();
        String executedScriptsTableName = defaultDatabase.toCorrectCaseIdentifier(getString(PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME, getConfiguration()));
        String fileNameColumnName = defaultDatabase.toCorrectCaseIdentifier(getString(PROPERTY_FILE_NAME_COLUMN_NAME, getConfiguration()));
        int fileNameColumnSize = PropertyUtils.getInt(PROPERTY_FILE_NAME_COLUMN_SIZE, getConfiguration());
        String fileLastModifiedAtColumnName = defaultDatabase.toCorrectCaseIdentifier(getString(PROPERTY_FILE_LAST_MODIFIED_AT_COLUMN_NAME, getConfiguration()));
        String checksumColumnName = defaultDatabase.toCorrectCaseIdentifier(getString(PROPERTY_CHECKSUM_COLUMN_NAME, getConfiguration()));
        int checksumColumnSize = PropertyUtils.getInt(PROPERTY_CHECKSUM_COLUMN_SIZE, getConfiguration());
        String executedAtColumnName = defaultDatabase.toCorrectCaseIdentifier(getString(PROPERTY_EXECUTED_AT_COLUMN_NAME, getConfiguration()));
        int executedAtColumnSize = PropertyUtils.getInt(PROPERTY_EXECUTED_AT_COLUMN_SIZE, getConfiguration());
        String succeededColumnName = defaultDatabase.toCorrectCaseIdentifier(getString(PROPERTY_SUCCEEDED_COLUMN_NAME, getConfiguration()));
        DateFormat timestampFormat = new SimpleDateFormat(getString(PROPERTY_TIMESTAMP_FORMAT, getConfiguration()));
        String targetDatabasePrefix = getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, getConfiguration());
        String qualifierPrefix = getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, getConfiguration());
        Set<Qualifier> registeredQualifiers = factoryWithDatabaseContext.createQualifiers(getStringList(PROPERTY_QUALIFIERS, getConfiguration()));
        Set<Qualifier> patchQualifiers = factoryWithDatabaseContext.createQualifiers(getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, getConfiguration()));
        String postProcessingScriptsDirName = getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, getConfiguration());
        ScriptIndexes baselineRevision = factoryWithDatabaseContext.getBaselineRevision();

        return new DefaultExecutedScriptInfoSource(autoCreateExecutedScriptsTable, executedScriptsTableName, fileNameColumnName, fileNameColumnSize,
                fileLastModifiedAtColumnName, checksumColumnName, checksumColumnSize,
                executedAtColumnName, executedAtColumnSize, succeededColumnName, timestampFormat, defaultDatabase,
                getSqlHandler(), targetDatabasePrefix, qualifierPrefix, registeredQualifiers, patchQualifiers, postProcessingScriptsDirName, baselineRevision);
    }

}
