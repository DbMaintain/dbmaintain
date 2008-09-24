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
package org.dbmaintain.util.ant;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.tools.ant.Task;
import org.dbmaintain.dbsupport.DbSupport;
import org.dbmaintain.dbsupport.DefaultSQLHandler;
import org.dbmaintain.util.ConfigUtils;
import org.dbmaintain.util.DbMaintainConfigurationLoader;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
abstract public class BaseDatabaseTask extends Task {

	protected DbSupport createDbSupport(DatabaseType database) {
    	Properties configuration = new DbMaintainConfigurationLoader().getDefaultConfiguration();
    	
    	BasicDataSource dataSource = new BasicDataSource();
    	dataSource.setDriverClassName(database.getDriverClassName());
    	dataSource.setUrl(database.getUrl());
    	dataSource.setUsername(database.getUserName());
    	dataSource.setPassword(database.getPassword());
    	
    	String defaultSchemaName;
		Set<String> schemaNames;
		if (database.getDefaultSchemaName() == null) {
			defaultSchemaName = database.getUserName();
			schemaNames = Collections.singleton(defaultSchemaName);
		} else {
			defaultSchemaName = database.getDefaultSchemaName();
			schemaNames = database.getSchemaNames();
		}
		
		DbSupport dbSupport = ConfigUtils.getInstanceOf(DbSupport.class, configuration, database.getDialect());
		dbSupport.init(configuration, new DefaultSQLHandler(), dataSource, database.getName(), defaultSchemaName, schemaNames);
		return dbSupport;
	}
}
