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
package org.dbmaintain.structure.sequence.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.database.Database;
import org.dbmaintain.database.Databases;
import org.dbmaintain.structure.sequence.SequenceUpdater;
import org.dbmaintain.util.DbMaintainException;

import java.util.Set;

/**
 * Implementation of {@link SequenceUpdater}. All sequences and identity columns that have a value lower than the given value.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultSequenceUpdater implements SequenceUpdater {


    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultSequenceUpdater.class);

    /* The lowest acceptable sequence value */
    protected long lowestAcceptableSequenceValue;
    protected Databases databases;


    public DefaultSequenceUpdater(long lowestAcceptableSequenceValue, Databases databases) {
        this.lowestAcceptableSequenceValue = lowestAcceptableSequenceValue;
        this.databases = databases;
    }

    /**
     * Updates all database sequences and identity columns to a sufficiently high value, so that test data be inserted
     * easily.
     */
    public void updateSequences() {
        for (Database database : databases.getDatabases()) {
            for (String schemaName : database.getSchemaNames()) {
                logger.info("Updating sequences and identity columns in database " + (database.getDatabaseName() != null ? database.getDatabaseName() +
                        ", and schema " : "schema ") + schemaName);
                incrementSequencesWithLowValue(database, schemaName);
                incrementIdentityColumnsWithLowValue(database, schemaName);
            }
        }
    }


    /**
     * Increments all sequences in the given schema whose value is too low.
     *
     * @param database   The database support, not null
     * @param schemaName The schema, not null
     */
    private void incrementSequencesWithLowValue(Database database, String schemaName) {
        if (!database.supportsSequences()) {
            return;
        }
        Set<String> sequenceNames = database.getSequenceNames(schemaName);
        for (String sequenceName : sequenceNames) {
            if (database.getSequenceValue(schemaName, sequenceName) < lowestAcceptableSequenceValue) {
                logger.debug("Incrementing value for sequence " + sequenceName + " in database schema " + schemaName);
                database.incrementSequenceToValue(schemaName, sequenceName, lowestAcceptableSequenceValue);
            }
        }
    }


    /**
     * Increments the next value for identity columns in the given schema whose next value is too low
     *
     * @param database   The database support, not null
     * @param schemaName The schema, not null
     */
    private void incrementIdentityColumnsWithLowValue(Database database, String schemaName) {
        if (!database.supportsIdentityColumns()) {
            return;
        }
        Set<String> tableNames = database.getTableNames(schemaName);
        for (String tableName : tableNames) {
            Set<String> identityColumnNames = database.getIdentityColumnNames(schemaName, tableName);
            for (String identityColumnName : identityColumnNames) {
                try {
                    database.incrementIdentityColumnToValue(schemaName, tableName, identityColumnName, lowestAcceptableSequenceValue);
                    logger.debug("Incrementing value for identity column " + identityColumnName + " in database schema " + schemaName);

                } catch (DbMaintainException e) {
                    // primary key is not an identity column
                    // skip column
                }
            }
        }
    }

}
