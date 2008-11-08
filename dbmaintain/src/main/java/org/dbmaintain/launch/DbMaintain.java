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
package org.dbmaintain.launch;

import java.io.File;
import java.util.List;

import org.dbmaintain.DbMaintainer;
import org.dbmaintain.clean.DBCleaner;
import org.dbmaintain.clean.DBClearer;
import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptSource;
import org.dbmaintain.script.impl.JarScriptContainer;
import org.dbmaintain.structure.ConstraintsDisabler;
import org.dbmaintain.structure.SequenceUpdater;
import org.dbmaintain.version.ExecutedScriptInfoSource;

/**
 * Class that exposes all available DbMaintain operations.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 *
 */
public class DbMaintain {
    
    private PropertiesDbMaintainConfigurer dbMaintainConfigurer;
    
    /**
     * @param dbMaintainConfigurer Configurer that will be used to configure all DbMaintain service objects
     */
    public DbMaintain(PropertiesDbMaintainConfigurer dbMaintainConfigurer) {
        this.dbMaintainConfigurer = dbMaintainConfigurer;
    }


    /**
     * Creates a jar file containing all scripts in all configured script locations
     * 
     * @param jarFileName The name of the jar file to create
     */
    public void createDbJar(String jarFileName) {
        ScriptSource scriptSource = dbMaintainConfigurer.createScriptSource();
        List<Script> allScripts = scriptSource.getAllUpdateScripts();
        allScripts.addAll(scriptSource.getPostProcessingScripts());
        JarScriptContainer jarScriptContainer = dbMaintainConfigurer.createJarScriptContainer(allScripts);
        jarScriptContainer.writeToJarFile(new File(jarFileName));
    }

    
    /**
     * Updates the database to the latest version.
     */
    public void updateDatabase() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.updateDatabase();
    }

    
    /**
     * Marks the database as up-to-date, without executing any script. You can use this operation to prepare 
     * an existing database to be managed by DbMaintain, or after having manually fixed a problem.
     */
    public void markDatabaseAsUptodate() {
        DbMaintainer dbMaintainer = dbMaintainConfigurer.createDbMaintainer();
        dbMaintainer.markDatabaseAsUptodate();
    }

    /**
     * Removes all database items, and empties the DBMAINTAIN_SCRIPTS table.
     */
    public void clearDatabase() {
        DBClearer dbClearer = dbMaintainConfigurer.createDbClearer();
        dbClearer.clearDatabase();
        ExecutedScriptInfoSource executedScriptInfoSource = dbMaintainConfigurer.createExecutedScriptInfoSource();
        executedScriptInfoSource.clearAllExecutedScripts();
    }

    /**
     * Removes the data of all database tables, except for the DBMAINTAIN_SCRIPTS table.
     */
    public void cleanDatabase() {
        DBCleaner dbCleaner = dbMaintainConfigurer.createDbCleaner();
        dbCleaner.cleanDatabase();
    }

    /**
     * Disables or drops all foreign key and not null constraints.
     */
    public void disableConstraints() {
        ConstraintsDisabler constraintsDisabler = dbMaintainConfigurer.createConstraintsDisabler();
        constraintsDisabler.disableConstraints();
    }

    /**
     * Updates all sequences and identity columns to a minimum value.
     */
    public void updateSequences() {
        SequenceUpdater sequenceUpdater = dbMaintainConfigurer.createSequenceUpdater();
        sequenceUpdater.updateSequences();
    }

}
