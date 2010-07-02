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
package org.dbmaintain.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.DbMaintainer;
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.clear.impl.DefaultDBClearer;
import org.dbmaintain.dbsupport.*;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.format.ScriptUpdatesFormatter;
import org.dbmaintain.script.IncludeExcludeQualifierEvaluator;
import org.dbmaintain.script.Qualifier;
import org.dbmaintain.script.QualifierEvaluator;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.impl.ArchiveScriptLocation;
import org.dbmaintain.script.impl.FileSystemScriptLocation;
import org.dbmaintain.script.impl.ScriptLocation;
import org.dbmaintain.script.impl.ScriptRepository;
import org.dbmaintain.scriptparser.ScriptParserFactory;
import org.dbmaintain.scriptrunner.ScriptRunner;
import org.dbmaintain.scriptrunner.impl.SqlPlusScriptRunner;
import org.dbmaintain.scriptrunner.impl.db2.Db2ScriptRunner;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.structure.impl.DefaultConstraintsDisabler;
import org.dbmaintain.structure.impl.DefaultSequenceUpdater;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.dbmaintain.config.ConfigUtils.getConfiguredClass;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.getString;
import static org.dbmaintain.config.PropertyUtils.getStringList;
import static org.dbmaintain.dbsupport.DbItemIdentifier.*;
import static org.dbmaintain.dbsupport.DbItemType.*;
import static org.dbmaintain.util.ReflectionUtils.createInstanceOfType;

/**
 * Configures all dbmaintain objects using dependency injection (constructor injection)
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class PropertiesDbMaintainConfigurer {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(PropertiesDbMaintainConfigurer.class);

    protected Properties configuration;
    protected SQLHandler sqlHandler;
    protected DbSupports dbSupports;

    protected Set<Qualifier> registeredQualifiers;


    /**
     * Creates a configurer without any database support.
     * Use this when you don't need database access, e.g. when creating a scripts jar.
     *
     * @param configuration the properties that define the entire configuration of dbmaintain
     */
    public PropertiesDbMaintainConfigurer(Properties configuration) {
        this(configuration, null, null);
    }


    /**
     * Constructor for PropertiesDbMaintainConfigurer that passes in a custom defaultDbSupport and nameDbSupportMap
     *
     * @param configuration the properties that define the rest of the configuration of dbmaintain
     * @param dbSupports    the databases supports, not null
     * @param sqlHandler    handles all queries and updates to the database
     */
    public PropertiesDbMaintainConfigurer(Properties configuration, DbSupports dbSupports, SQLHandler sqlHandler) {
        this.configuration = configuration;
        this.sqlHandler = sqlHandler;
        this.dbSupports = dbSupports;
    }


    public DbMaintainer createDbMaintainer() {
        ScriptIndexes baseLineRevision = getBaselineRevision();
        ScriptRunner scriptRunner = createScriptRunner();
        ScriptRepository scriptRepository = createScriptRepository(baseLineRevision);
        ExecutedScriptInfoSource executedScriptInfoSource = createExecutedScriptInfoSource(baseLineRevision);

        boolean cleanDbEnabled = PropertyUtils.getBoolean(PROPERTY_CLEANDB, configuration);
        boolean fromScratchEnabled = PropertyUtils.getBoolean(PROPERTY_FROM_SCRATCH_ENABLED, configuration);
        boolean hasItemsToPreserve = !getItemsToPreserve().isEmpty();
        boolean useScriptFileLastModificationDates = PropertyUtils.getBoolean(PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES, configuration);
        boolean allowOutOfSequenceExecutionOfPatchScripts = PropertyUtils.getBoolean(PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, configuration);
        boolean disableConstraintsEnabled = PropertyUtils.getBoolean(PROPERTY_DISABLE_CONSTRAINTS, configuration);
        boolean updateSequencesEnabled = PropertyUtils.getBoolean(PROPERTY_UPDATE_SEQUENCES, configuration);
        long maxNrOfCharsWhenLoggingScriptContent = PropertyUtils.getLong(PROPERTY_MAX_NR_CHARS_WHEN_LOGGING_SCRIPT_CONTENT, configuration);

        DBCleaner dbCleaner = createDbCleaner();
        DBClearer dbClearer = createDbClearer();
        ConstraintsDisabler constraintsDisabler = createConstraintsDisabler();
        SequenceUpdater sequenceUpdater = createSequenceUpdater();
        ScriptUpdatesFormatter scriptUpdatesFormatter = createScriptUpdatesFormatter();

        Class<DbMaintainer> clazz = getConfiguredClass(DbMaintainer.class, configuration);
        return createInstanceOfType(clazz, false,
                new Class<?>[]{ScriptRunner.class, ScriptRepository.class, ExecutedScriptInfoSource.class, boolean.class, boolean.class, boolean.class,
                        boolean.class, boolean.class, boolean.class, boolean.class, DBClearer.class, DBCleaner.class, ConstraintsDisabler.class, SequenceUpdater.class,
                        ScriptUpdatesFormatter.class, SQLHandler.class, long.class},
                new Object[]{scriptRunner, scriptRepository, executedScriptInfoSource, fromScratchEnabled, hasItemsToPreserve, useScriptFileLastModificationDates,
                        allowOutOfSequenceExecutionOfPatchScripts, cleanDbEnabled, disableConstraintsEnabled, updateSequencesEnabled,
                        dbClearer, dbCleaner, constraintsDisabler, sequenceUpdater, scriptUpdatesFormatter, sqlHandler, maxNrOfCharsWhenLoggingScriptContent});
    }

    public QualifierEvaluator createQualifierEvaluator(Set<ScriptLocation> scriptLocations) {
        Set<Qualifier> includedQualifiers = createQualifiers(getStringList(PROPERTY_INCLUDED_QUALIFIERS, configuration, false));
        ensureQualifiersRegistered(includedQualifiers, scriptLocations);
        Set<Qualifier> excludedQualifiers = createQualifiers(getStringList(PROPERTY_EXCLUDED_QUALIFIERS, configuration, false));
        ensureQualifiersRegistered(excludedQualifiers, scriptLocations);
        return new IncludeExcludeQualifierEvaluator(includedQualifiers, excludedQualifiers);
    }


    protected Set<Qualifier> createQualifiers(List<String> qualifierNames) {
        Set<Qualifier> qualifiers = new HashSet<Qualifier>(qualifierNames.size());
        for (String qualifierName : qualifierNames) {
            qualifiers.add(new Qualifier(qualifierName));
        }
        return qualifiers;
    }

    protected void ensureQualifiersRegistered(Set<Qualifier> qualifiers, Set<ScriptLocation> scriptLocations) {
        for (Qualifier qualifier : qualifiers) {
            if (!getRegisteredQualifiers(scriptLocations).contains(qualifier)) {
                throw new IllegalArgumentException(qualifier + " is not registered");
            }
        }
    }

    protected Set<Qualifier> getRegisteredQualifiers(Set<ScriptLocation> scriptLocations) {
        if (registeredQualifiers == null) {
            registeredQualifiers = createQualifiers(getStringList(PROPERTY_QUALIFIERS, configuration));
            for (ScriptLocation scriptLocation : scriptLocations) {
                registeredQualifiers.addAll(scriptLocation.getRegisteredQualifiers());
            }
        }
        return registeredQualifiers;
    }

    public ScriptRunner createScriptRunner() {
        Map<String, ScriptParserFactory> databaseDialectScriptParserFactoryMap = createDatabaseDialectScriptParserFactoryMap();
        Class<ScriptRunner> clazz = getConfiguredClass(ScriptRunner.class, configuration);
        if (clazz.equals(SqlPlusScriptRunner.class)) {
            return createSqlPlusScriptRunner(clazz);
        }
        if (clazz.equals(Db2ScriptRunner.class)) {
            return createDb2ScriptRunner(clazz);
        }
        return createInstanceOfType(clazz, false, new Class<?>[]{Map.class, DbSupports.class, SQLHandler.class}, new Object[]{databaseDialectScriptParserFactoryMap, dbSupports, sqlHandler});
    }

    protected ScriptRunner createSqlPlusScriptRunner(Class<ScriptRunner> clazz) {
        String sqlPlusCommand = PropertyUtils.getString(PROPERTY_SQL_PLUS_COMMAND, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[]{DbSupports.class, String.class}, new Object[]{dbSupports, sqlPlusCommand});
    }

    protected ScriptRunner createDb2ScriptRunner(Class<ScriptRunner> clazz) {
        String db2Command = PropertyUtils.getString(PROPERTY_DB2_COMMAND, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[]{DbSupports.class, String.class}, new Object[]{dbSupports, db2Command});
    }


    public Map<String, ScriptParserFactory> createDatabaseDialectScriptParserFactoryMap() {
        Map<String, ScriptParserFactory> databaseDialectScriptParserClassMap = new HashMap<String, ScriptParserFactory>();
        boolean backSlashEscapingEnabled = PropertyUtils.getBoolean(PROPERTY_BACKSLASH_ESCAPING_ENABLED, configuration);
        for (String databaseDialect : getDatabaseDialectsInUse()) {
            Class<? extends ScriptParserFactory> scriptParserFactoryClass = getConfiguredClass(ScriptParserFactory.class, configuration, databaseDialect);
            ScriptParserFactory factory = createInstanceOfType(scriptParserFactoryClass, false, new Class<?>[]{boolean.class}, new Object[]{backSlashEscapingEnabled});
            databaseDialectScriptParserClassMap.put(databaseDialect, factory);
        }
        return databaseDialectScriptParserClassMap;
    }

    protected Set<String> getDatabaseDialectsInUse() {
        Set<String> dialects = new HashSet<String>();
        for (DbSupport dbSupport : dbSupports.getDbSupports()) {
            if (dbSupport != null) {
                dialects.add(dbSupport.getSupportedDatabaseDialect());
            }
        }
        return dialects;
    }


    public ScriptRepository createScriptRepository(ScriptIndexes baseLineRevision) {
        Set<String> scriptLocationIndicators = new HashSet<String>(getStringList(PROPERTY_SCRIPT_LOCATIONS, configuration));
        Set<ScriptLocation> scriptLocations = new HashSet<ScriptLocation>();
        for (String scriptLocationIndicator : scriptLocationIndicators) {
            scriptLocations.add(createScriptLocation(scriptLocationIndicator));
        }
        QualifierEvaluator qualifierEvaluator = createQualifierEvaluator(scriptLocations);
        return new ScriptRepository(scriptLocations, qualifierEvaluator, baseLineRevision);
    }


    public ArchiveScriptLocation createArchiveScriptLocation(SortedSet<Script> scripts) {
        String scriptEncoding = getString(PROPERTY_SCRIPT_ENCODING, configuration);
        String postProcessingScriptDirName = getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, configuration);
        Set<Qualifier> registeredQualifiers = createQualifiers(getStringList(PROPERTY_QUALIFIERS, configuration));
        Set<Qualifier> patchQualifiers = createQualifiers(getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, configuration));
        String qualifierPrefix = getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, configuration);
        String targetDatabasePrefix = getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, configuration);
        Set<String> scriptFileExtensions = new HashSet<String>(getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, configuration));
        return new ArchiveScriptLocation(scripts, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions);
    }

    public ScriptLocation createScriptLocation(String scriptLocation) {
        String scriptEncoding = getString(PROPERTY_SCRIPT_ENCODING, configuration);
        String postProcessingScriptDirName = getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, configuration);
        Set<Qualifier> registeredQualifiers = createQualifiers(getStringList(PROPERTY_QUALIFIERS, configuration));
        Set<Qualifier> patchQualifiers = createQualifiers(getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, configuration));
        String qualifierPrefix = getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, configuration);
        String targetDatabasePrefix = getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, configuration);
        Set<String> scriptFileExtensions = new HashSet<String>(getStringList(PROPERTY_SCRIPT_FILE_EXTENSIONS, configuration));
        File scriptLocationFile = new File(scriptLocation);
        if (scriptLocationFile.isDirectory()) {
            return new FileSystemScriptLocation(scriptLocationFile, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions);
        } else {
            return new ArchiveScriptLocation(scriptLocationFile, scriptEncoding, postProcessingScriptDirName, registeredQualifiers, patchQualifiers, qualifierPrefix, targetDatabasePrefix, scriptFileExtensions);
        }
    }

    public ExecutedScriptInfoSource createExecutedScriptInfoSource(ScriptIndexes baselineRevision) {
        boolean autoCreateExecutedScriptsTable = PropertyUtils.getBoolean(PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, configuration);

        DbSupport defaultDbSupport = dbSupports.getDefaultDbSupport();
        String executedScriptsTableName = defaultDbSupport.toCorrectCaseIdentifier(getString(PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME, configuration));
        String fileNameColumnName = defaultDbSupport.toCorrectCaseIdentifier(getString(PROPERTY_FILE_NAME_COLUMN_NAME, configuration));
        int fileNameColumnSize = PropertyUtils.getInt(PROPERTY_FILE_NAME_COLUMN_SIZE, configuration);
        String fileLastModifiedAtColumnName = defaultDbSupport.toCorrectCaseIdentifier(getString(PROPERTY_FILE_LAST_MODIFIED_AT_COLUMN_NAME, configuration));
        String checksumColumnName = defaultDbSupport.toCorrectCaseIdentifier(getString(PROPERTY_CHECKSUM_COLUMN_NAME, configuration));
        int checksumColumnSize = PropertyUtils.getInt(PROPERTY_CHECKSUM_COLUMN_SIZE, configuration);
        String executedAtColumnName = defaultDbSupport.toCorrectCaseIdentifier(getString(PROPERTY_EXECUTED_AT_COLUMN_NAME, configuration));
        int executedAtColumnSize = PropertyUtils.getInt(PROPERTY_EXECUTED_AT_COLUMN_SIZE, configuration);
        String succeededColumnName = defaultDbSupport.toCorrectCaseIdentifier(getString(PROPERTY_SUCCEEDED_COLUMN_NAME, configuration));
        DateFormat timestampFormat = new SimpleDateFormat(getString(PROPERTY_TIMESTAMP_FORMAT, configuration));
        String targetDatabasePrefix = getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, configuration);
        String qualifierPrefix = getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, configuration);
        Set<Qualifier> registeredQualifiers = createQualifiers(getStringList(PROPERTY_QUALIFIERS, configuration));
        Set<Qualifier> patchQualifiers = createQualifiers(getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, configuration));
        String postProcessingScriptsDirName = getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, configuration);

        Class<ExecutedScriptInfoSource> clazz = getConfiguredClass(ExecutedScriptInfoSource.class, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[]{boolean.class, String.class, String.class, int.class, String.class, String.class,
                int.class, String.class, int.class, String.class, DateFormat.class, DbSupport.class, SQLHandler.class, String.class, String.class,
                Set.class, Set.class, String.class, ScriptIndexes.class},

                new Object[]{autoCreateExecutedScriptsTable, executedScriptsTableName, fileNameColumnName, fileNameColumnSize,
                        fileLastModifiedAtColumnName, checksumColumnName, checksumColumnSize,
                        executedAtColumnName, executedAtColumnSize, succeededColumnName, timestampFormat, dbSupports.getDefaultDbSupport(),
                        sqlHandler, targetDatabasePrefix, qualifierPrefix, registeredQualifiers, patchQualifiers, postProcessingScriptsDirName, baselineRevision});
    }


    public DBCleaner createDbCleaner() {
        Set<DbItemIdentifier> itemsToPreserve = getItemsToPreserve();
        itemsToPreserve.add(getExecutedScriptsTable());

        Class<DBCleaner> clazz = getConfiguredClass(DBCleaner.class, configuration);
        return createInstanceOfType(clazz, false,
                new Class<?>[]{DbSupports.class, Set.class, SQLHandler.class},
                new Object[]{dbSupports, itemsToPreserve, sqlHandler});
    }

    protected Set<DbItemIdentifier> getItemsToPreserve() {
        Set<DbItemIdentifier> itemsToPreserve = getSchemasToPreserve(PROPERTY_PRESERVE_SCHEMAS);
        itemsToPreserve.addAll(getSchemasToPreserve(PROPERTY_PRESERVE_DATA_SCHEMAS));
        itemsToPreserve.addAll(getItemsToPreserve(TABLE, PROPERTY_PRESERVE_TABLES));
        itemsToPreserve.addAll(getItemsToPreserve(TABLE, PROPERTY_PRESERVE_DATA_TABLES));
        return itemsToPreserve;
    }


    public DBClearer createDbClearer() {
        Set<DbItemIdentifier> itemsToPreserve = new HashSet<DbItemIdentifier>();
        itemsToPreserve.add(getExecutedScriptsTable());
        itemsToPreserve.addAll(getSchemasToPreserve(PROPERTY_PRESERVE_SCHEMAS));
        addItemsToPreserve(TABLE, PROPERTY_PRESERVE_TABLES, itemsToPreserve);
        addItemsToPreserve(VIEW, PROPERTY_PRESERVE_VIEWS, itemsToPreserve);
        addItemsToPreserve(MATERIALIZED_VIEW, PROPERTY_PRESERVE_MATERIALIZED_VIEWS, itemsToPreserve);
        addItemsToPreserve(SYNONYM, PROPERTY_PRESERVE_SYNONYMS, itemsToPreserve);
        addItemsToPreserve(SEQUENCE, PROPERTY_PRESERVE_SEQUENCES, itemsToPreserve);
        addItemsToPreserve(TRIGGER, PROPERTY_PRESERVE_TRIGGERS, itemsToPreserve);
        addItemsToPreserve(TYPE, PROPERTY_PRESERVE_TYPES, itemsToPreserve);

        Class<DefaultDBClearer> clazz = getConfiguredClass(DefaultDBClearer.class, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[]{DbSupports.class, Set.class}, new Object[]{dbSupports, itemsToPreserve});
    }


    protected DbItemIdentifier getExecutedScriptsTable() {
        String executedScriptsTableName = getString(PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME, configuration);
        DbSupport defaultDbSupport = dbSupports.getDefaultDbSupport();
        return getItemIdentifier(TABLE, defaultDbSupport.getDefaultSchemaName(), executedScriptsTableName, defaultDbSupport, true);
    }


    public ScriptUpdatesFormatter createScriptUpdatesFormatter() {
        return new ScriptUpdatesFormatter();
    }

    private void addItemsToPreserve(DbItemType dbItemType, String itemsToPreserveProperty, Set<DbItemIdentifier> itemsToPreserve) {
        List<String> items = getStringList(itemsToPreserveProperty, configuration);
        for (String itemToPreserve : items) {
            DbItemIdentifier itemIdentifier = parseItemIdentifier(dbItemType, itemToPreserve, dbSupports);
            itemsToPreserve.add(itemIdentifier);
        }
    }


    /**
     * Gets the list of schemas to preserve. The case is correct if necessary. Quoting an identifier
     * makes it case sensitive. If requested, the identifiers will be qualified with the default schema name if no
     * schema name is used as prefix.
     *
     * @param propertyName The name of the property that defines the items, not null
     * @return The set of items, not null
     */
    protected Set<DbItemIdentifier> getSchemasToPreserve(String propertyName) {
        Set<DbItemIdentifier> result = new HashSet<DbItemIdentifier>();
        List<String> schemasToPreserve = getStringList(propertyName, configuration);
        for (String schemaToPreserve : schemasToPreserve) {
            DbItemIdentifier itemIdentifier = parseSchemaIdentifier(schemaToPreserve, dbSupports);
            result.add(itemIdentifier);
        }
        return result;
    }

    /**
     * Gets the list of items to preserve. The case is corrected if necessary. Quoting an identifier
     * makes it case sensitive. If requested, the identifiers will be qualified with the default schema name if no
     * schema name is used as prefix.
     *
     * @param dbItemType   the type of database items to preserve, not null
     * @param propertyName the name of the property that defines the items, not null
     * @return The set of items, not null
     */
    protected Set<DbItemIdentifier> getItemsToPreserve(DbItemType dbItemType, String propertyName) {
        Set<DbItemIdentifier> result = new HashSet<DbItemIdentifier>();
        List<String> itemsToPreserve = getStringList(propertyName, configuration);
        for (String itemToPreserve : itemsToPreserve) {
            DbItemIdentifier itemIdentifier = parseItemIdentifier(dbItemType, itemToPreserve, dbSupports);
            result.add(itemIdentifier);
        }
        return result;
    }


    public ConstraintsDisabler createConstraintsDisabler() {
        return new DefaultConstraintsDisabler(dbSupports);
    }

    public SequenceUpdater createSequenceUpdater() {
        long lowestAcceptableSequenceValue = PropertyUtils.getLong(PROPERTY_LOWEST_ACCEPTABLE_SEQUENCE_VALUE, configuration);
        return new DefaultSequenceUpdater(lowestAcceptableSequenceValue, dbSupports);
    }

    public ScriptIndexes getBaselineRevision() {
        String baseLineRevisionString = PropertyUtils.getString(PROPERTY_BASELINE_REVISION, null, configuration);
        if (isBlank(baseLineRevisionString)) {
            return null;
        }
        logger.info("The baseline revision is set to " + baseLineRevisionString + ". All script with a lower revision will be ignored");
        return new ScriptIndexes(baseLineRevisionString);
    }

}
