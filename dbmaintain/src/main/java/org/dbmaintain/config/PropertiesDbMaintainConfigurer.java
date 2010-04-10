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

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clear.DBClearer;
import org.dbmaintain.clear.impl.DefaultDBClearer;
import org.dbmaintain.dbsupport.*;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;
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
import org.dbmaintain.util.DbMaintainException;

import javax.sql.DataSource;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.dbmaintain.config.ConfigUtils.getConfiguredClass;
import static org.dbmaintain.config.DbMaintainProperties.*;
import static org.dbmaintain.config.PropertyUtils.*;
import static org.dbmaintain.dbsupport.DbItemType.*;
import static org.dbmaintain.dbsupport.DbMaintainDataSource.createDataSource;
import static org.dbmaintain.util.ReflectionUtils.createInstanceOfType;

/**
 * Configures all dbmaintain objects using dependency injection (constructor injection)
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class PropertiesDbMaintainConfigurer {

    protected Properties configuration;

    protected SQLHandler sqlHandler;

    protected DbSupport defaultDbSupport;
    protected Map<String, DbSupport> nameDbSupportMap;

    protected Set<Qualifier> registeredQualifiers;


    /**
     * @param configuration the properties that define the entire configuration of dbmaintain
     * @param sqlHandler    handles all queries and updates to the database
     */
    public PropertiesDbMaintainConfigurer(Properties configuration, SQLHandler sqlHandler) {
        this.configuration = configuration;
        this.sqlHandler = sqlHandler;
    }


    /**
     * Constructor for PropertiesDbMaintainConfigurer that passes in a custom defaultDbSupport and nameDbSupportMap
     *
     * @param configuration       the properties that define the rest of the configuration of dbmaintain
     * @param defaultDatabaseName the default database, not null
     * @param nameDatabaseInfoMap the databases in a map with its name as key and the database info as value (null value if the database is disabled), not null
     * @param sqlHandler          handles all queries and updates to the database
     */
    public PropertiesDbMaintainConfigurer(Properties configuration, String defaultDatabaseName, Map<String, DatabaseInfo> nameDatabaseInfoMap, SQLHandler sqlHandler) {
        this.configuration = configuration;
        this.sqlHandler = sqlHandler;
        initDbSupports(defaultDatabaseName, nameDatabaseInfoMap);
    }


    public DbMaintainer createDbMaintainer() {
        ScriptRunner scriptRunner = createScriptRunner();
        ScriptRepository scriptRepository = createScriptRepository();
        ExecutedScriptInfoSource executedScriptInfoSource = createExecutedScriptInfoSource();

        boolean cleanDbEnabled = PropertyUtils.getBoolean(PROPERTY_CLEANDB, configuration);
        boolean fromScratchEnabled = PropertyUtils.getBoolean(PROPERTY_FROM_SCRATCH_ENABLED, configuration);
        boolean hasItemsToPreserve = getItemsToPreserve().size() > 0 || getSchemasToPreserve().size() > 0;
        boolean useScriptFileLastModificationDates = PropertyUtils.getBoolean(PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES, configuration);
        boolean allowOutOfSequenceExecutionOfPatchScripts = PropertyUtils.getBoolean(PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION, configuration);
        boolean disableConstraintsEnabled = PropertyUtils.getBoolean(PROPERTY_DISABLE_CONSTRAINTS, configuration);
        boolean updateSequencesEnabled = PropertyUtils.getBoolean(PROPERTY_UPDATE_SEQUENCES, configuration);

        DBCleaner dbCleaner = createDbCleaner();
        DBClearer dbClearer = createDbClearer();
        ConstraintsDisabler constraintsDisabler = createConstraintsDisabler();
        SequenceUpdater sequenceUpdater = createSequenceUpdater();
        ScriptUpdatesFormatter scriptUpdatesFormatter = createScriptUpdatesFormatter();

        Class<DbMaintainer> clazz = getConfiguredClass(DbMaintainer.class, configuration);
        return createInstanceOfType(clazz, false,
                new Class<?>[]{ScriptRunner.class, ScriptRepository.class, ExecutedScriptInfoSource.class, boolean.class, boolean.class, boolean.class,
                        boolean.class, boolean.class, boolean.class, boolean.class,
                        DBClearer.class, DBCleaner.class, ConstraintsDisabler.class, SequenceUpdater.class, ScriptUpdatesFormatter.class, SQLHandler.class},
                new Object[]{scriptRunner, scriptRepository, executedScriptInfoSource, fromScratchEnabled, hasItemsToPreserve, useScriptFileLastModificationDates,
                        allowOutOfSequenceExecutionOfPatchScripts, cleanDbEnabled, disableConstraintsEnabled, updateSequencesEnabled,
                        dbClearer, dbCleaner, constraintsDisabler, sequenceUpdater, scriptUpdatesFormatter, sqlHandler});
    }

    public QualifierEvaluator createQualifierEvaluator() {
        Set<Qualifier> includedQualifiers = createQualifiers(getStringList(PROPERTY_INCLUDED_QUALIFIERS, configuration, false));
        ensureQualifiersRegistered(includedQualifiers);
        Set<Qualifier> excludedQualifiers = createQualifiers(getStringList(PROPERTY_EXCLUDED_QUALIFIERS, configuration, false));
        ensureQualifiersRegistered(excludedQualifiers);
        return new IncludeExcludeQualifierEvaluator(includedQualifiers, excludedQualifiers);
    }

    protected Set<Qualifier> createQualifiers(List<String> qualifierNames) {
        Set<Qualifier> qualifiers = new HashSet<Qualifier>(qualifierNames.size());
        for (String qualifierName : qualifierNames) {
            qualifiers.add(new Qualifier(qualifierName));
        }
        return qualifiers;
    }

    protected void ensureQualifiersRegistered(Set<Qualifier> qualifiers) {
        for (Qualifier qualifier : qualifiers) {
            if (!getRegisteredQualifiers().contains(qualifier)) {
                throw new IllegalArgumentException("Qualifier " + qualifier + " is not registered");
            }
        }
    }

    protected Set<Qualifier> getRegisteredQualifiers() {
        if (registeredQualifiers == null) {
            registeredQualifiers = createQualifiers(getStringList(PROPERTY_QUALIFIERS, configuration));
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
        return createInstanceOfType(clazz, false, new Class<?>[]{Map.class, DbSupport.class, Map.class, SQLHandler.class}, new Object[]{databaseDialectScriptParserFactoryMap, getDefaultDbSupport(), getNameDbSupportMap(), sqlHandler});
    }

    protected ScriptRunner createSqlPlusScriptRunner(Class<ScriptRunner> clazz) {
        String sqlPlusCommand = PropertyUtils.getString(PROPERTY_SQL_PLUS_COMMAND, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[]{DbSupport.class, Map.class, String.class}, new Object[]{getDefaultDbSupport(), getNameDbSupportMap(), sqlPlusCommand});
    }

    protected ScriptRunner createDb2ScriptRunner(Class<ScriptRunner> clazz) {
        String db2Command = PropertyUtils.getString(PROPERTY_DB2_COMMAND, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[]{DbSupport.class, Map.class, String.class}, new Object[]{getDefaultDbSupport(), getNameDbSupportMap(), db2Command});
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
        for (DbSupport dbSupport : getNameDbSupportMap().values()) {
            if (dbSupport != null) {
                dialects.add(dbSupport.getSupportedDatabaseDialect());
            }
        }
        return dialects;
    }


    public ScriptRepository createScriptRepository() {
        Set<String> scriptLocationIndicators = new HashSet<String>(getStringList(PROPERTY_SCRIPT_LOCATIONS, configuration));
        Set<ScriptLocation> scriptLocations = new HashSet<ScriptLocation>();
        for (String scriptLocationIndicator : scriptLocationIndicators) {
            scriptLocations.add(createScriptLocation(scriptLocationIndicator));
        }
        QualifierEvaluator qualifierEvaluator = createQualifierEvaluator();
        return new ScriptRepository(scriptLocations, qualifierEvaluator);
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

    public ExecutedScriptInfoSource createExecutedScriptInfoSource() {
        boolean autoCreateExecutedScriptsTable = PropertyUtils.getBoolean(PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE, configuration);
        String executedScriptsTableName = getDefaultDbSupport().toCorrectCaseIdentifier(getString(PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME, configuration));
        String fileNameColumnName = getDefaultDbSupport().toCorrectCaseIdentifier(getString(PROPERTY_FILE_NAME_COLUMN_NAME, configuration));
        int fileNameColumnSize = PropertyUtils.getInt(PROPERTY_FILE_NAME_COLUMN_SIZE, configuration);
        String fileLastModifiedAtColumnName = getDefaultDbSupport().toCorrectCaseIdentifier(getString(PROPERTY_FILE_LAST_MODIFIED_AT_COLUMN_NAME, configuration));
        String checksumColumnName = getDefaultDbSupport().toCorrectCaseIdentifier(getString(PROPERTY_CHECKSUM_COLUMN_NAME, configuration));
        int checksumColumnSize = PropertyUtils.getInt(PROPERTY_CHECKSUM_COLUMN_SIZE, configuration);
        String executedAtColumnName = getDefaultDbSupport().toCorrectCaseIdentifier(getString(PROPERTY_EXECUTED_AT_COLUMN_NAME, configuration));
        int executedAtColumnSize = PropertyUtils.getInt(PROPERTY_EXECUTED_AT_COLUMN_SIZE, configuration);
        String succeededColumnName = getDefaultDbSupport().toCorrectCaseIdentifier(getString(PROPERTY_SUCCEEDED_COLUMN_NAME, configuration));
        DateFormat timestampFormat = new SimpleDateFormat(getString(PROPERTY_TIMESTAMP_FORMAT, configuration));
        String targetDatabasePrefix = getString(PROPERTY_SCRIPT_TARGETDATABASE_PREFIX, configuration);
        String qualifierPrefix = getString(PROPERTY_SCRIPT_QUALIFIER_PREFIX, configuration);
        Set<Qualifier> registeredQualifiers = createQualifiers(getStringList(PROPERTY_QUALIFIERS, configuration));
        Set<Qualifier> patchQualifiers = createQualifiers(getStringList(PROPERTY_SCRIPT_PATCH_QUALIFIERS, configuration));
        String postProcessingScriptsDirname = getString(PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME, configuration);

        Class<ExecutedScriptInfoSource> clazz = getConfiguredClass(ExecutedScriptInfoSource.class, configuration);
        return createInstanceOfType(clazz, false, new Class<?>[]{boolean.class, String.class, String.class, int.class, String.class, String.class,
                int.class, String.class, int.class, String.class, DateFormat.class, DbSupport.class, SQLHandler.class, String.class, String.class,
                Set.class, Set.class, String.class},

                new Object[]{autoCreateExecutedScriptsTable, executedScriptsTableName, fileNameColumnName, fileNameColumnSize,
                        fileLastModifiedAtColumnName, checksumColumnName, checksumColumnSize,
                        executedAtColumnName, executedAtColumnSize, succeededColumnName, timestampFormat, getDefaultDbSupport(),
                        sqlHandler, targetDatabasePrefix, qualifierPrefix, registeredQualifiers, patchQualifiers, postProcessingScriptsDirname});
    }

    public DBCleaner createDbCleaner() {
        Set<DbItemIdentifier> schemasToPreserve = getSchemasToPreserve();
        Set<DbItemIdentifier> itemsToPreserve = getItemsToPreserve();

        String executedScriptsTableName = getString(PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME, configuration);
        itemsToPreserve.add(DbItemIdentifier.getItemIdentifier(TABLE, getDefaultDbSupport().getDefaultSchemaName(), executedScriptsTableName, getDefaultDbSupport()));

        Class<DBCleaner> clazz = getConfiguredClass(DBCleaner.class, configuration);

        return createInstanceOfType(clazz, false,
                new Class<?>[]{Map.class, Set.class, Set.class, SQLHandler.class},
                new Object[]{getNameDbSupportMap(), schemasToPreserve, itemsToPreserve, sqlHandler});
    }


    protected Set<DbItemIdentifier> getItemsToPreserve() {
        Set<DbItemIdentifier> itemsToPreserve = getItemsToPreserve(TABLE, PROPERTY_PRESERVE_TABLES);
        itemsToPreserve.addAll(getItemsToPreserve(TABLE, PROPERTY_PRESERVE_DATA_TABLES));
        return itemsToPreserve;
    }

    protected Set<DbItemIdentifier> getSchemasToPreserve() {
        Set<DbItemIdentifier> schemasToPreserve = getSchemasToPreserve(PROPERTY_PRESERVE_SCHEMAS);
        schemasToPreserve.addAll(getSchemasToPreserve(PROPERTY_PRESERVE_DATA_SCHEMAS));
        return schemasToPreserve;
    }


    public DBClearer createDbClearer() {
        Class<DefaultDBClearer> clazz = getConfiguredClass(DefaultDBClearer.class, configuration);
        DefaultDBClearer dbClearer = createInstanceOfType(clazz, false, new Class<?>[]{Map.class}, new Object[]{getNameDbSupportMap()});

        Set<DbItemIdentifier> schemasToPreserve = getSchemasToPreserve(PROPERTY_PRESERVE_SCHEMAS);
        for (DbItemIdentifier schemaToPreserve : schemasToPreserve) {
            dbClearer.addItemToPreserve(schemaToPreserve, true);
        }
        String executedScriptsTableName = getString(PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME, configuration);
        dbClearer.addItemToPreserve(DbItemIdentifier.parseItemIdentifier(TABLE, executedScriptsTableName, getDefaultDbSupport(), getNameDbSupportMap()), false);
        addItemsToPreserve(dbClearer, TABLE, PROPERTY_PRESERVE_TABLES);
        addItemsToPreserve(dbClearer, VIEW, PROPERTY_PRESERVE_VIEWS);
        addItemsToPreserve(dbClearer, MATERIALZED_VIEW, PROPERTY_PRESERVE_MATERIALIZED_VIEWS);
        addItemsToPreserve(dbClearer, SYNONYM, PROPERTY_PRESERVE_SYNONYMS);
        addItemsToPreserve(dbClearer, SEQUENCE, PROPERTY_PRESERVE_SEQUENCES);
        addItemsToPreserve(dbClearer, TRIGGER, PROPERTY_PRESERVE_TRIGGERS);
        addItemsToPreserve(dbClearer, TYPE, PROPERTY_PRESERVE_TYPES);
        return dbClearer;
    }

    public ScriptUpdatesFormatter createScriptUpdatesFormatter() {
        return new ScriptUpdatesFormatter();
    }

    private void addItemsToPreserve(DefaultDBClearer defaultDbClearer, DbItemType dbItemType, String itemsToPreserveProperty) {
        List<String> itemsToPreserve = getStringList(itemsToPreserveProperty, configuration);
        for (String itemToPreserve : itemsToPreserve) {
            DbItemIdentifier itemIdentifier = DbItemIdentifier.parseItemIdentifier(dbItemType, itemToPreserve, getDefaultDbSupport(), getNameDbSupportMap());
            defaultDbClearer.addItemToPreserve(itemIdentifier, true);
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
            DbItemIdentifier itemIdentifier = DbItemIdentifier.parseSchemaIdentifier(schemaToPreserve, getDefaultDbSupport(), getNameDbSupportMap());
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
            DbItemIdentifier itemIdentifier = DbItemIdentifier.parseItemIdentifier(dbItemType, itemToPreserve, getDefaultDbSupport(), getNameDbSupportMap());
            result.add(itemIdentifier);
        }
        return result;
    }


    public ConstraintsDisabler createConstraintsDisabler() {
        return new DefaultConstraintsDisabler(getDbSupports());
    }

    public SequenceUpdater createSequenceUpdater() {
        long lowestAcceptableSequenceValue = PropertyUtils.getLong(PROPERTY_LOWEST_ACCEPTABLE_SEQUENCE_VALUE, configuration);
        return new DefaultSequenceUpdater(lowestAcceptableSequenceValue, getNameDbSupportMap().values());
    }


    /**
     * @return a set with the available DbSupport instances, one for each configured database
     */
    protected Set<DbSupport> getDbSupports() {
        Set<DbSupport> result = new HashSet<DbSupport>();
        for (DbSupport dbSupport : getNameDbSupportMap().values()) {
            if (dbSupport != null) {
                result.add(dbSupport);
            }
        }
        return result;
    }


    public DbSupport createDbSupport(DatabaseInfo databaseInfo) {
        DataSource dataSource = createDataSource(databaseInfo);
        String databaseDialect = databaseInfo.getDialect();
        String customIdentifierQuoteString = getCustomIdentifierQuoteString(databaseDialect);
        StoredIdentifierCase customStoredIdentifierCase = getCustomStoredIdentifierCase(databaseDialect);

        Class<DbSupport> clazz = getConfiguredClass(DbSupport.class, configuration, databaseDialect);
        return createInstanceOfType(clazz, false,
                new Class<?>[]{DatabaseInfo.class, DataSource.class, SQLHandler.class, String.class, StoredIdentifierCase.class},
                new Object[]{databaseInfo, dataSource, sqlHandler, customIdentifierQuoteString, customStoredIdentifierCase}
        );
    }


    protected StoredIdentifierCase getCustomStoredIdentifierCase(String databaseDialect) {
        String storedIdentifierCasePropertyValue = getString(PROPERTY_STORED_IDENTIFIER_CASE + "." + databaseDialect, configuration);
        if ("lower_case".equals(storedIdentifierCasePropertyValue)) {
            return StoredIdentifierCase.LOWER_CASE;
        } else if ("upper_case".equals(storedIdentifierCasePropertyValue)) {
            return StoredIdentifierCase.UPPER_CASE;
        } else if ("mixed_case".equals(storedIdentifierCasePropertyValue)) {
            return StoredIdentifierCase.MIXED_CASE;
        } else if ("auto".equals(storedIdentifierCasePropertyValue)) {
            return null;
        }
        throw new DbMaintainException("Unknown value " + storedIdentifierCasePropertyValue + " for property " + PROPERTY_STORED_IDENTIFIER_CASE + ". It should be one of lower_case, upper_case, mixed_case or auto.");
    }


    protected String getCustomIdentifierQuoteString(String databaseDialect) {
        String identifierQuoteStringPropertyValue = getString(PROPERTY_IDENTIFIER_QUOTE_STRING + '.' + databaseDialect, configuration);
        if ("none".equals(identifierQuoteStringPropertyValue)) {
            return "";
        }
        if ("auto".equals(identifierQuoteStringPropertyValue)) {
            return null;
        }
        return identifierQuoteStringPropertyValue;
    }


    public DatabaseInfo getUnnamedDatabaseInfo() {
        String driverClassName = getString(PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END, configuration);
        String url = getString(PROPERTY_DATABASE_START + '.' + PROPERTY_URL_END, configuration);
        String userName = getString(PROPERTY_DATABASE_START + '.' + PROPERTY_USERNAME_END, configuration);
        String password = getString(PROPERTY_DATABASE_START + '.' + PROPERTY_PASSWORD_END, "", configuration);
        String databaseDialect = getString(PROPERTY_DATABASE_START + '.' + PROPERTY_DIALECT_END, configuration);
        List<String> schemaNames = getStringList(PROPERTY_DATABASE_START + '.' + PROPERTY_SCHEMANAMES_END, configuration);
        if (schemaNames.isEmpty()) {
            throw new DbMaintainException("No value found for property " + PROPERTY_DATABASE_START + '.' + PROPERTY_SCHEMANAMES_END);
        }
        return new DatabaseInfo("<no-name>", databaseDialect, driverClassName, url, userName, password, schemaNames);
    }

    /**
     * @param databaseName The name that identifies the database, not null
     * @return a DataSource that connects with the database as configured for the given database name
     */
    public DatabaseInfo getDatabaseInfo(String databaseName) {
        String driverClassNamePropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String urlPropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String userNamePropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String passwordPropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String databaseDialectPropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_DIALECT_END;
        String schemaNamesListPropertyName = PROPERTY_DATABASE_START + '.' + PROPERTY_SCHEMANAMES_END;
        String customDriverClassNamePropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_DRIVERCLASSNAME_END;
        String customUrlPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_URL_END;
        String customUserNamePropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_USERNAME_END;
        String customPasswordPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_PASSWORD_END;
        String customSchemaNamesPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_SCHEMANAMES_END;
        String customDatabaseDialectPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_DIALECT_END;
        String customSchemaNamesListPropertyName = PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_SCHEMANAMES_END;

        if (!(containsProperty(customDriverClassNamePropertyName, configuration) ||
                containsProperty(customUrlPropertyName, configuration) ||
                containsProperty(customUserNamePropertyName, configuration) ||
                containsProperty(customPasswordPropertyName, configuration) ||
                containsProperty(customSchemaNamesPropertyName, configuration))) {
            throw new DbMaintainException("No custom database properties defined for database " + databaseName);
        }
        String driverClassName = containsProperty(customDriverClassNamePropertyName, configuration) ? getString(customDriverClassNamePropertyName, configuration) : getString(driverClassNamePropertyName, configuration);
        String url = containsProperty(customUrlPropertyName, configuration) ? getString(customUrlPropertyName, configuration) : getString(urlPropertyName, configuration);
        String userName = containsProperty(customUserNamePropertyName, configuration) ? getString(customUserNamePropertyName, configuration) : getString(userNamePropertyName, configuration);
        String password = containsProperty(customPasswordPropertyName, configuration) ? getString(customPasswordPropertyName, configuration) : getString(passwordPropertyName, configuration);
        String databaseDialect = containsProperty(customDatabaseDialectPropertyName, configuration) ? getString(customDatabaseDialectPropertyName, configuration) : getString(databaseDialectPropertyName, configuration);
        List<String> schemaNames = containsProperty(customSchemaNamesListPropertyName, configuration) ? getStringList(customSchemaNamesListPropertyName, configuration) : getStringList(schemaNamesListPropertyName, configuration);
        if (schemaNames.isEmpty()) {
            throw new DbMaintainException("No value found for property " + schemaNamesListPropertyName);
        }
        return new DatabaseInfo(databaseName, databaseDialect, driverClassName, url, userName, password, schemaNames);
    }


    protected void initDbSupportsFromProperties() {
        nameDbSupportMap = new HashMap<String, DbSupport>();

        List<String> databaseNames = getStringList(PROPERTY_DATABASE_NAMES, configuration);
        if (databaseNames.isEmpty()) {
            defaultDbSupport = createDbSupport(getUnnamedDatabaseInfo());
            nameDbSupportMap.put(null, defaultDbSupport);
        } else {
            for (String databaseName : databaseNames) {
                DatabaseInfo databaseInfo = null;
                if (isDatabaseIncluded(databaseName)) {
                    databaseInfo = getDatabaseInfo(databaseName);
                }
                DbSupport dbSupport = createDbSupport(databaseInfo);
                nameDbSupportMap.put(databaseName, dbSupport);
                if (defaultDbSupport == null) {
                    defaultDbSupport = dbSupport;
                }
            }
        }
    }

    protected void initDbSupports(String defaultDatabaseName, Map<String, DatabaseInfo> nameDatabaseInfoMap) {
        nameDbSupportMap = new HashMap<String, DbSupport>();
        for (Map.Entry<String, DatabaseInfo> entry : nameDatabaseInfoMap.entrySet()) {
            String name = entry.getKey();
            DatabaseInfo databaseInfo = entry.getValue();
            if (databaseInfo == null) {
                nameDbSupportMap.put(name, null);
            } else {
                nameDbSupportMap.put(name, createDbSupport(databaseInfo));
            }
        }
        defaultDbSupport = nameDbSupportMap.get(defaultDatabaseName);
    }


    /**
     * @param databaseName the logical name that identifies the database
     * @return whether the database with the given name is included in the set of database to be updated by dbmaintain
     */
    protected boolean isDatabaseIncluded(String databaseName) {
        return PropertyUtils.getBoolean(PROPERTY_DATABASE_START + '.' + databaseName + '.' + PROPERTY_INCLUDED_END, true, configuration);
    }


    public DbSupport getDbSupport(String name) {
        if (name == null) {
            return getDefaultDbSupport();
        }
        DbSupport dbSupport = getNameDbSupportMap().get(name);
        if (dbSupport == null) {
            throw new DbMaintainException("No test database configured with the name '" + name + "'");
        }
        return dbSupport;
    }


    public DbSupport getDefaultDbSupport() {
        if (defaultDbSupport == null) {
            initDbSupportsFromProperties();
        }
        return defaultDbSupport;
    }


    public Map<String, DbSupport> getNameDbSupportMap() {
        if (nameDbSupportMap == null) {
            initDbSupportsFromProperties();
        }
        return nameDbSupportMap;
    }

}
