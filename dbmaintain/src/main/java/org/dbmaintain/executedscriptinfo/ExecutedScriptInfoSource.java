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
package org.dbmaintain.executedscriptinfo;

import java.util.Set;

import org.dbmaintain.script.ExecutedScript;


/**
 * Interface that enables registering which scripts were executed on the database and retrieving this information afterwards. 
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface ExecutedScriptInfoSource {

    
    /**
     * Registers the fact that the given script has been executed on the database
     * 
     * @param executedScript The script that was executed on the database
     */
    void registerExecutedScript(ExecutedScript executedScript);

    
    /**
     * Updates the given registered script
     * 
     * @param executedScript
     */
    void updateExecutedScript(ExecutedScript executedScript);
    
    
    /**
     * Clears all script executions that have been registered. After having invoked this method, 
     * {@link #getExecutedScripts()} will return an empty set.
     */
    void clearAllExecutedScripts();
    
    
    /**
     * @return All scripts that were registered as being executed on the database
     */
    Set<ExecutedScript> getExecutedScripts();


    /**
     * Remove the given executed script from the executed scripts
     *
     * @param executedScript The executed script, which is no longer part of the executed scripts
     */
    void deleteExecutedScript(ExecutedScript executedScript);
}
