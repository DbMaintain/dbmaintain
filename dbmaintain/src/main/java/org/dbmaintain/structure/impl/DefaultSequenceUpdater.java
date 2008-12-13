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
package org.dbmaintain.structure.impl;

import java.util.Collection;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.util.DbMaintainException;

/**
 * Implementation of {@link SequenceUpdater}. All sequences and identity columns that have a value lower than the value
 * defined by {@link #PROPERTY_LOWEST_ACCEPTABLE_SEQUENCE_VALUE} are set to this value.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class DefaultSequenceUpdater implements SequenceUpdater {

    
    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultSequenceUpdater.class);

    /* The lowest acceptable sequence value */
    protected long lowestAcceptableSequenceValue;

    protected Collection<DbSupport> dbSupports;

    
    /**
     * @param lowestAcceptableSequenceValue
     * @param dbSupports
     */
    public DefaultSequenceUpdater(long lowestAcceptableSequenceValue, Collection<DbSupport> dbSupports) {
        this.lowestAcceptableSequenceValue = lowestAcceptableSequenceValue;
        this.dbSupports = dbSupports;
    }


    /**
     * Updates all database sequences and identity columns to a sufficiently high value, so that test data be inserted
     * easily.
     */
    public void updateSequences() {
        for (DbSupport dbSupport : dbSupports) {
        	for (String schemaName : dbSupport.getSchemaNames()) {
	            logger.info("Updating sequences and identity columns in database " + (dbSupport.getDatabaseName() != null ? dbSupport.getDatabaseName() + 
	        			", and schema " : "schema ") + schemaName);
	            incrementSequencesWithLowValue(dbSupport, schemaName);
	            incrementIdentityColumnsWithLowValue(dbSupport, schemaName);
        	}
        }
    }


    /**
     * Increments all sequences whose value is too low.
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    private void incrementSequencesWithLowValue(DbSupport dbSupport, String schemaName) {
        if (!dbSupport.supportsSequences()) {
            return;
        }
        Set<String> sequenceNames = dbSupport.getSequenceNames(schemaName);
        for (String sequenceName : sequenceNames) {
            if (dbSupport.getSequenceValue(schemaName, sequenceName) < lowestAcceptableSequenceValue) {
                logger.debug("Incrementing value for sequence " + sequenceName + " in database schema " + schemaName);
                dbSupport.incrementSequenceToValue(schemaName, sequenceName, lowestAcceptableSequenceValue);
            }
        }
    }


    /**
     * Increments the next value for identity columns whose next value is too low
     *
     * @param dbSupport The database support, not null
     * @param schemaName 
     */
    private void incrementIdentityColumnsWithLowValue(DbSupport dbSupport, String schemaName) {
        if (!dbSupport.supportsIdentityColumns()) {
            return;
        }
        Set<String> tableNames = dbSupport.getTableNames(schemaName);
        for (String tableName : tableNames) {
            Set<String> identityColumnNames = dbSupport.getIdentityColumnNames(schemaName, tableName);
            for (String identityColumnName : identityColumnNames) {
                try {
                    dbSupport.incrementIdentityColumnToValue(schemaName, tableName, identityColumnName, lowestAcceptableSequenceValue);
                    logger.debug("Incrementing value for identity column " + identityColumnName + " in database schema " + schemaName);

                } catch (DbMaintainException e) {
                    // primary key is not an identity column
                    // skip column
                }
            }
        }
    }

}
