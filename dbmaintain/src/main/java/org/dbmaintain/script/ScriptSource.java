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
package org.dbmaintain.script;

import org.dbmaintain.version.ScriptIndexes;

import java.util.List;
import java.util.Set;

/**
 * A source that provides scripts for updating the database to a given state.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface ScriptSource {


    /**
     * @return a list of all available update scripts, in the order in which they must be executed on the database. 
     * These scripts can be used to completely recreate the database from scratch. Not null
     */
    List<Script> getAllUpdateScripts();


    /**
     * Returns a list of scripts including the ones that:
     * <ol><li>have a higher version than the given version</li>
     * <li>are unversioned, and they weren't yet applied on the database</li>
     * <li>are unversioned, and their contents differ from the one currently applied to the database</li>
     * <p/>
     * The scripts are returned in the order in which they should be executed.
     *
     * @param highestExecutedScriptVersion The highest version of the versioned scripts that were already applied to the database
     * @param alreadyExecutedScripts The scripts which were already executed on the database
     * @return The new scripts.
     */
    List<Script> getNewScripts(ScriptIndexes highestExecutedScriptVersion, Set<ExecutedScript> alreadyExecutedScripts);


    /**
     * Returns the scripts that have a version index equal to or lower than the index specified by the given version object 
     * that have been modified according to the given already executed scripts.
     *
     * @param currentVersion The current database version, not null
     * @param alreadyExecutedScripts 
     * @return True if an existing script has been modified, false otherwise
     */
    boolean isIncrementalScriptModified(ScriptIndexes currentVersion, Set<ExecutedScript> alreadyExecutedScripts);


    /**
     * Gets a list of all post processing scripts.
     * <p/>
     * The scripts are returned in the order in which they should be executed.
     *
     * @return All the postprocessing code scripts, not null
     */
    List<Script> getPostProcessingScripts();

}
