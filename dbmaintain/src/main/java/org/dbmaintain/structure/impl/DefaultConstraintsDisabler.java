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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.structure.ConstraintsDisabler;

import java.util.Collection;

/**
 * Default implementation of {@link ConstraintsDisabler}.
 * This will disable all foreign key, check and not-null constraints on the configured database schemas.
 * Primary key constraints will not be disabled.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 * @author Bart Vermeiren
 */
public class DefaultConstraintsDisabler implements ConstraintsDisabler {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(DefaultConstraintsDisabler.class);

    /* The database supports to disable the constraints for */
    protected Collection<DbSupport> dbSupports;


    /**
     * Creates the constraints disabler.
     *
     * @param dbSupports The database supports to disable the constraints for, not null
     */
    public DefaultConstraintsDisabler(Collection<DbSupport> dbSupports) {
        this.dbSupports = dbSupports;
    }


    /**
     * Disable every foreign key or not-null constraint
     */
    public void disableConstraints() {
        for (DbSupport dbSupport : dbSupports) {
            for (String schemaName : dbSupport.getSchemaNames()) {
                logger.info("Disabling constraints in database schema " + schemaName);

                // first disable referential constraints to avoid conflicts
                disableReferentialConstraints(schemaName, dbSupport);
                // disable not-null and check constraints
                disableValueConstraints(schemaName, dbSupport);
            }
        }
    }


    /**
     * Disables all referential constraints (e.g. foreign keys) on all tables in the schema
     *
     * @param schemaName The schema name, not null
     * @param dbSupport  The dbSupport for the database, not null
     */
    protected void disableReferentialConstraints(String schemaName, DbSupport dbSupport) {
        try {
            dbSupport.disableReferentialConstraints(schemaName);
        } catch (Throwable t) {
            logger.error("Unable to remove referential constraints.", t);
        }
    }


    /**
     * Disables all value constraints (e.g. not null) on all tables in the schema
     *
     * @param schemaName The schema name, not null
     * @param dbSupport  The dbSupport for the database, not null
     */
    protected void disableValueConstraints(String schemaName, DbSupport dbSupport) {
        try {
            dbSupport.disableValueConstraints(schemaName);
        } catch (Throwable t) {
            logger.error("Unable to remove value constraints.", t);
        }
    }
}