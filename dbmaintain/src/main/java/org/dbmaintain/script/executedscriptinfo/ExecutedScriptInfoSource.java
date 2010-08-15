/*
 * Copyright DbMaintain.org
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
package org.dbmaintain.script.executedscriptinfo;

import org.dbmaintain.script.ExecutedScript;
import org.dbmaintain.script.Script;

import java.util.Set;


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
     * @param executedScript The script to update, not null
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

    /**
     * Registers the fact that the script that was originally executed has been renamed.
     *
     * @param executedScript  the original executed script that still refers to the original script
     * @param renamedToScript the script to which the original script has been renamed
     */
    void renameExecutedScript(ExecutedScript executedScript, Script renamedToScript);

    /**
     * Removes all previously registered executed postprocessing scripts
     */
    void deleteAllExecutedPostprocessingScripts();

    /**
     * Marks the failed scripts in the executed scripts table as successful.
     */
    void markErrorScriptsAsSuccessful();

    /**
     * Removes the failed scripts in the executed scripts table.
     */
    void removeErrorScripts();
}
