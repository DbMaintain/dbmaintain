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

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DbMaintainProperties {

    /**
     * Logical names for the databases. This property is only required if there's more than one database configured.
     */
    public static final String PROPERTY_DATABASE_NAMES = "databases.names";

    /**
     * Start of all property names that concern the configuration of a database
     */
    public static final String PROPERTY_DATABASE_START = "database";

    /**
     * End of property names that indicate a database driver classname
     */
    public static final String PROPERTY_DRIVERCLASSNAME_END = "driverClassName";

    /**
     * End of property names that indicate a database url
     */
    public static final String PROPERTY_URL_END = "url";

    /**
     * End of property names that indicate a database username
     */
    public static final String PROPERTY_USERNAME_END = "userName";

    /**
     * End of property names that indicate a database password
     */
    public static final String PROPERTY_PASSWORD_END = "password";

    /**
     * End of property names that indicate whether the database is included: i.e. it is active and must be updated
     */
    public static final String PROPERTY_INCLUDED_END = "included";

    /**
     * Property key of the SQL dialect of the underlying DBMS implementation
     */
    public static final String PROPERTY_DIALECT_END = "dialect";

    /**
     * Property key for the database schema names
     */
    public static final String PROPERTY_SCHEMANAMES_END = "schemaNames";

    /**
     * Property key for the default identifier casing (lower_case, upper_case,
     * mixed_case, auto)
     */
    public static final String PROPERTY_STORED_IDENTIFIER_CASE = "database.storedIndentifierCase";

    /**
     * Property key for the default identifier quote string (empty value for not
     * supported, auto)
     */
    public static final String PROPERTY_IDENTIFIER_QUOTE_STRING = "database.identifierQuoteString";

    /**
     * Property indicating if deleting all data from all tables before updating is enabled
     */
    public static final String PROPERTY_CLEANDB = "dbMaintainer.cleanDb";

    /**
     * Property indicating if updating the database from scratch is enabled
     */
    public static final String PROPERTY_FROM_SCRATCH_ENABLED = "dbMaintainer.fromScratch.enabled";

    /**
     * Property indicating if the database constraints should org disabled after updating the database
     */
    public static final String PROPERTY_DISABLE_CONSTRAINTS = "dbMaintainer.disableConstraints";

    /**
     * Property indicating if the database constraints should org disabled after updating the database
     */
    public static final String PROPERTY_UPDATE_SEQUENCES = "dbMaintainer.updateSequences";

    /**
     * Property that indicates if a data set DTD or XSD is to be generated or not
     */
    public static final String PROPERTY_GENERATE_DATA_SET_STRUCTURE = "dbMaintainer.generateDataSetStructure";

    /* Property key for the lowest acceptable sequence value */
    public static final String PROPERTY_LOWEST_ACCEPTABLE_SEQUENCE_VALUE = "sequenceUpdater.lowestAcceptableSequenceValue";

    /**
     * The key of the property that specifies of which schemas nothing should be dropped
     */
    public static final String PROPERTY_PRESERVE_SCHEMAS = "dbMaintainer.preserve.schemas";

    /**
     * The key of the property that specifies which tables should not be dropped
     */
    public static final String PROPERTY_PRESERVE_TABLES = "dbMaintainer.preserve.tables";

    /**
     * The key of the property that specifies which views should not be dropped
     */
    public static final String PROPERTY_PRESERVE_VIEWS = "dbMaintainer.preserve.views";

    /**
     * The key of the property that specifies which materialized views should not be dropped
     */
    public static final String PROPERTY_PRESERVE_MATERIALIZED_VIEWS = "dbMaintainer.preserve.materializedViews";

    /**
     * The key of the property that specifies which synonyms should not be dropped
     */
    public static final String PROPERTY_PRESERVE_SYNONYMS = "dbMaintainer.preserve.synonyms";

    /**
     * The key of the property that specifies which sequences should not be dropped
     */
    public static final String PROPERTY_PRESERVE_SEQUENCES = "dbMaintainer.preserve.sequences";

    /**
     * The key of the property that specifies which triggers should not be dropped
     */
    public static final String PROPERTY_PRESERVE_TRIGGERS = "dbMaintainer.preserve.triggers";

    /**
     * The key of the property that specifies which types should not be dropped
     */
    public static final String PROPERTY_PRESERVE_TYPES = "dbMaintainer.preserve.types";

    /**
     * Property key for schemas in which none of the tables should be cleaned
     */
    public static final String PROPERTY_PRESERVE_DATA_SCHEMAS = "dbMaintainer.preserveDataOnly.schemas";

    /**
     * Property key for the tables that should not be cleaned
     */
    public static final String PROPERTY_PRESERVE_DATA_TABLES = "dbMaintainer.preserveDataOnly.tables";

    /* The key of the property that specifies the database table in which the DB version is stored */
    public static final String PROPERTY_EXECUTED_SCRIPTS_TABLE_NAME = "dbMaintainer.executedScriptsTableName";

    /* The key of the property that specifies the column in which the script filenames are stored */
    public static final String PROPERTY_FILE_NAME_COLUMN_NAME = "dbMaintainer.fileNameColumnName";
    public static final String PROPERTY_FILE_NAME_COLUMN_SIZE = "dbMaintainer.fileNameColumnSize";

    /* The key of the property that specifies the column in which the last modification timestamp is stored */
    public static final String PROPERTY_FILE_LAST_MODIFIED_AT_COLUMN_NAME = "dbMaintainer.fileLastModifiedAtColumnName";

    /* The key of the property that specifies the column in which the last modification timestamp is stored */
    public static final String PROPERTY_CHECKSUM_COLUMN_NAME = "dbMaintainer.checksumColumnName";
    public static final String PROPERTY_CHECKSUM_COLUMN_SIZE = "dbMaintainer.checksumColumnSize";

    /* The key of the property that specifies the column in which is stored whether the last update succeeded. */
    public static final String PROPERTY_EXECUTED_AT_COLUMN_NAME = "dbMaintainer.executedAtColumnName";
    public static final String PROPERTY_EXECUTED_AT_COLUMN_SIZE = "dbMaintainer.executedAtColumnSize";

    /* The key of the property that specifies the column in which is stored whether the last update succeeded. */
    public static final String PROPERTY_SUCCEEDED_COLUMN_NAME = "dbMaintainer.succeededColumnName";

    /* The key of the property that specifies whether the executed scripts table should be created automatically. */
    public static final String PROPERTY_AUTO_CREATE_DBMAINTAIN_SCRIPTS_TABLE = "dbMaintainer.autoCreateDbMaintainScriptsTable";

    public static final String PROPERTY_TIMESTAMP_FORMAT = "dbMaintainer.timestampFormat";

    /**
     * Property key for the directory in which the script files are located
     */
    public static final String PROPERTY_SCRIPT_LOCATIONS = "dbMaintainer.script.locations";

    /**
     * Property key for the extension of the script files
     */
    public static final String PROPERTY_SCRIPT_FILE_EXTENSIONS = "dbMaintainer.script.fileExtensions";

    /**
     * Property key for the directory in which the code script files are located
     */
    public static final String PROPERTY_POSTPROCESSINGSCRIPT_DIRNAME = "dbMaintainer.postProcessingScript.directoryName";

    /**
     * Property key for the patch indicator. I.e. the keyword to use in the filename to indicate that the script is a patch script.
     */
    public static final String PROPERTY_SCRIPT_PATCH_QUALIFIERS = "dbMaintainer.script.patch.qualifiers";

    /**
     * The key for the property that specifies that patch scripts can be executed out of sequence
     */
    public static final String PROPERTY_PATCH_ALLOWOUTOFSEQUENCEEXECUTION = "dbMaintainer.allowOutOfSequenceExecutionOfPatches";

    public static final String PROPERTY_USESCRIPTFILELASTMODIFICATIONDATES = "dbMaintainer.useScriptFileLastModificationDates";

    public static final String PROPERTY_SCRIPT_ENCODING = "dbMaintainer.script.encoding";

    public static final String PROPERTY_SCRIPT_TARGETDATABASE_PREFIX = "dbMaintainer.script.targetDatabase.prefix";

    public static final String PROPERTY_SCRIPT_QUALIFIER_PREFIX = "dbMaintainer.script.qualifier.prefix";

    public static final String PROPERTY_BACKSLASH_ESCAPING_ENABLED = "dbMaintainer.script.backSlashEscapingEnabled";

    /**
     * Private constructor to prevent instantiation
     */
    private DbMaintainProperties() {}
}
