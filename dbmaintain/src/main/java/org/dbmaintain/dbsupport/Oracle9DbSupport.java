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
package org.dbmaintain.dbsupport;

import org.dbmaintain.util.StoredIdentifierCase;

import javax.sql.DataSource;

import java.util.Set;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class Oracle9DbSupport extends OracleDbSupport {

    /**
     * Creates support for a Oracle 9 database. Normally you don't need to use this class: OracleDbSupport
     * can find out the version automatically. Use this class only if your driver is not capable to 
     * retrieve the oracle version automatically.
     * 
     * @param databaseName 
     * @param dataSource 
     * @param defaultSchemaName 
     * @param schemaNames 
     * @param sqlHandler 
     * @param customIdentifierQuoteString 
     * @param customStoredIdentifierCase 
     */
    public Oracle9DbSupport(String databaseName, DataSource dataSource, String defaultSchemaName, 
            Set<String> schemaNames, SQLHandler sqlHandler, String customIdentifierQuoteString, StoredIdentifierCase customStoredIdentifierCase) {
        super(databaseName, "oracle9", dataSource, defaultSchemaName, schemaNames, sqlHandler, customIdentifierQuoteString, customStoredIdentifierCase);
    }
    
	@Override
	protected Integer getOracleMajorVersionNumber() {
		return 9;
	}

	
}
