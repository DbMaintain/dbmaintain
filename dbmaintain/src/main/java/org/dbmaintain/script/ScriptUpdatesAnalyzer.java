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

import org.dbmaintain.script.impl.ScriptRepository;
import static org.dbmaintain.script.ScriptUpdateType.*;
import static org.dbmaintain.script.ScriptUpdateType.POSTPROCESSING_SCRIPT_UPDATED;
import org.dbmaintain.executedscriptinfo.ExecutedScriptInfoSource;

import java.util.*;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 29-dec-2008
 */
public class ScriptUpdatesAnalyzer {

    private ScriptRepository scriptRepository;
    private ExecutedScriptInfoSource executedScriptInfoSource;
    private boolean useScriptFileLastModificationDates;
    private boolean allowOutOfSequenceExecutionOfPatchScripts;

    private SortedSet<ScriptUpdate> regularlyAddedOrModifiedScripts;
    private SortedSet<ScriptUpdate> irregularScriptUpdates;
    private SortedSet<ScriptUpdate> regularlyDeletedRepeatableScripts;
    private SortedSet<ScriptUpdate> regularlyAddedPatchScripts;
    private SortedSet<ScriptUpdate> regularPostprocessingScriptUpdates;
    private SortedSet<ScriptUpdate> regularlyRenamedScripts;

    public ScriptUpdatesAnalyzer(ScriptRepository scriptRepository, ExecutedScriptInfoSource executedScriptInfoSource,
                         boolean useScriptFileLastModificationDates, boolean allowOutOfSequenceExecutionOfPatchScripts) {
        this.scriptRepository = scriptRepository;
        this.executedScriptInfoSource = executedScriptInfoSource;
        this.useScriptFileLastModificationDates = useScriptFileLastModificationDates;
        this.allowOutOfSequenceExecutionOfPatchScripts = allowOutOfSequenceExecutionOfPatchScripts;
    }

    public ScriptUpdates calculateScriptUpdates() {
        regularlyAddedOrModifiedScripts = new TreeSet<ScriptUpdate>();
        irregularScriptUpdates = new TreeSet<ScriptUpdate>();
        regularlyDeletedRepeatableScripts = new TreeSet<ScriptUpdate>();
        regularlyAddedPatchScripts = new TreeSet<ScriptUpdate>();
        regularPostprocessingScriptUpdates = new TreeSet<ScriptUpdate>();
        regularlyAddedOrModifiedScripts = new TreeSet<ScriptUpdate>();
        regularlyRenamedScripts = new TreeSet<ScriptUpdate>();

        Map<String, ExecutedScript> scriptNameExecutedScriptMap = getScriptNameExecutedScriptMap();
        Map<String, Script> scriptNameScriptMap = getScriptNameScriptMap();
        Map<Script, ExecutedScript> scriptExecutedScriptMap = new HashMap<Script, ExecutedScript>();

        // Iterate over the already executed scripts, and find out whether there are any updates. We map the executed scripts
        // with the new script counterparts, to be able to verify afterwards if scripts have been renamed or deleted.
        for (String executedScriptName : scriptNameExecutedScriptMap.keySet()) {
            if (scriptNameScriptMap.containsKey(executedScriptName)) {
                // The script with this name still exists. We store this in the executedScriptScriptMap
                ExecutedScript executedScript = scriptNameExecutedScriptMap.get(executedScriptName);
                Script script = scriptNameScriptMap.get(executedScriptName);
                scriptExecutedScriptMap.put(script, executedScript);
                // Check if the content didn't change
                if (!executedScript.getScript().isScriptContentEqualTo(script, useScriptFileLastModificationDates)) {
                    if (script.isIncremental()) {
                        addIrregularScriptUpdate(INDEXED_SCRIPT_UPDATED, script);
                    } else if (script.isRepeatable()) {
                        addRegularScriptUpdate(REPEATABLE_SCRIPT_UPDATED, script);
                    } else if (script.isPostProcessingScript()) {
                        addPostprocessingScriptUpdate(POSTPROCESSING_SCRIPT_UPDATED, script);
                    }
                }
            }
        }

        Map<ExecutedScript, Script> renamedIndexedScripts = new HashMap<ExecutedScript, Script>();
        if (executedScriptInfoSource.getExecutedScripts().size() != scriptExecutedScriptMap.size()) {
            // There are executed scripts for which a script with the same name cannot be found anymore. We are going to find
            // out what has happened to the executed scripts that could not be mapped: were they renamed or were they deleted?
            Map<String, Set<Script>> checkSumScriptMap = getCheckSumScriptMap();
            for (String executedScriptName : scriptNameExecutedScriptMap.keySet()) {
                if (!scriptNameScriptMap.containsKey(executedScriptName)) {
                    // Find out if there's a script with the same content, which is not yet mapped with an executed script,
                    // in this case we conclude that the script has been renamed.
                    ExecutedScript executedScript = scriptNameExecutedScriptMap.get(executedScriptName);
                    SortedSet<Script> newScriptsWithSameContent = new TreeSet<Script>();
                    Set<Script> scriptsWithSameContent = checkSumScriptMap.get(executedScript.getScript().getCheckSum());
                    if (scriptsWithSameContent != null) {
                        for (Script scriptWithSameContent : scriptsWithSameContent) {
                            if (!scriptExecutedScriptMap.containsKey(scriptWithSameContent)) {
                                newScriptsWithSameContent.add(scriptWithSameContent);
                            }
                        }
                    }
                    if (newScriptsWithSameContent.size() == 1) {
                        Script newScriptWithSameContent = newScriptsWithSameContent.first();
                        if (executedScript.getScript().isIncremental() && newScriptWithSameContent.isIncremental()) {
                            scriptExecutedScriptMap.put(newScriptWithSameContent, executedScript);
                            renamedIndexedScripts.put(executedScript, newScriptWithSameContent);
                        } else if (executedScript.getScript().isRepeatable() && newScriptWithSameContent.isRepeatable()) {
                            scriptExecutedScriptMap.put(newScriptWithSameContent, executedScript);
                            addRegularScriptRename(REPEATABLE_SCRIPT_RENAMED, executedScript.getScript(), newScriptWithSameContent);
                        } else if (executedScript.getScript().isPostProcessingScript() && newScriptWithSameContent.isPostProcessingScript()) {
                            scriptExecutedScriptMap.put(newScriptWithSameContent, executedScript);
                            addPostprocessingScriptUpdate(POSTPROCESSING_SCRIPT_RENAMED, executedScript.getScript(), newScriptWithSameContent);
                        }
                    } else {
                        // We didn't find a script with the same content that isn't mapped with an executed script yet
                        Script deletedScript = scriptNameExecutedScriptMap.get(executedScriptName).getScript();
                        if (deletedScript.isIncremental()) {
                            addIrregularScriptUpdate(INDEXED_SCRIPT_DELETED, deletedScript);
                        } else if (deletedScript.isRepeatable()) {
                            addRepeatableScriptDeletion(deletedScript);
                        } else if (deletedScript.isPostProcessingScript()) {
                            addPostprocessingScriptUpdate(POSTPROCESSING_SCRIPT_DELETED, deletedScript);
                        }
                    }
                }
            }
            boolean sequenceOfIndexedScriptsChangedDueToRenames = sequenceOfIndexedScriptsChangedDueToRenames(renamedIndexedScripts);
            for (ExecutedScript renamedIndexedExecutedScript : renamedIndexedScripts.keySet()) {
                if (sequenceOfIndexedScriptsChangedDueToRenames) {
                    addIrregularScriptUpdate(ScriptUpdateType.INDEXED_SCRIPT_RENAMED_SCRIPT_SEQUENCE_CHANGED,
                            renamedIndexedExecutedScript.getScript(), renamedIndexedScripts.get(renamedIndexedExecutedScript));
                } else {
                    addRegularScriptRename(ScriptUpdateType.INDEXED_SCRIPT_RENAMED, renamedIndexedExecutedScript.getScript(), renamedIndexedScripts.get(renamedIndexedExecutedScript));
                }
            }
        }

        // Search for indexed scriptNames whose version < the current version, which are new or whose contents have changed
        Script scriptWithHighestScriptIndex = getExecutedScriptWithHighestScriptIndex(executedScriptInfoSource.getExecutedScripts());
        for (Script indexedScript : scriptRepository.getIndexedScripts()) {
            if (!scriptExecutedScriptMap.containsKey(indexedScript)) {
                if (scriptWithHighestScriptIndex == null || indexedScript.compareTo(scriptWithHighestScriptIndex) > 0) {
                    addRegularScriptUpdate(HIGHER_INDEX_SCRIPT_ADDED, indexedScript);
                } else {
                    if (indexedScript.isPatchScript()) {
                        if (allowOutOfSequenceExecutionOfPatchScripts) {
                            addRegularlyAddedPatchScript(LOWER_INDEX_PATCH_SCRIPT_ADDED, indexedScript);
                        } else {
                            addIrregularScriptUpdate(LOWER_INDEX_PATCH_SCRIPT_ADDED, indexedScript);
                        }
                    } else {
                        addIrregularScriptUpdate(LOWER_INDEX_NON_PATCH_SCRIPT_ADDED, indexedScript);
                    }
                }
            }
        }

        for (Script repeatableScript : scriptRepository.getRepeatableScripts()) {
            if (!scriptExecutedScriptMap.containsKey(repeatableScript)) {
                addRegularScriptUpdate(REPEATABLE_SCRIPT_ADDED, repeatableScript);
            }
        }

        for (Script postprocessingScript : scriptRepository.getPostProcessingScripts()) {
            if (!scriptExecutedScriptMap.containsKey(postprocessingScript)) {
                addPostprocessingScriptUpdate(POSTPROCESSING_SCRIPT_ADDED, postprocessingScript);
            } 
        }

        return new ScriptUpdates(regularlyAddedOrModifiedScripts, irregularScriptUpdates, regularlyDeletedRepeatableScripts, regularlyAddedPatchScripts,
                regularPostprocessingScriptUpdates, regularlyRenamedScripts);
    }


    protected boolean sequenceOfIndexedScriptsChangedDueToRenames(Map<ExecutedScript, Script> renamedIndexedScripts) {
        Iterator<Script> indexedScriptsIterator = scriptRepository.getIndexedScripts().iterator();
        for (ExecutedScript executedScript : executedScriptInfoSource.getExecutedScripts()) {
            Script scriptInNewSequence = renamedIndexedScripts.get(executedScript);
            if (scriptInNewSequence == null) {
                scriptInNewSequence = executedScript.getScript();
            }
            boolean foundScriptInNewSequenceInIndexedScripts = false;
            while (!foundScriptInNewSequenceInIndexedScripts) {
                if (!indexedScriptsIterator.hasNext()) {
                    return true;
                }
                if (scriptInNewSequence.equals(indexedScriptsIterator.next())) {
                    foundScriptInNewSequenceInIndexedScripts = true;
                }
            }
        }
        return false;
    }

    /**
     * @return The already executed scripts, as a map from scriptName => ExecutedScript
     */
    protected Map<String, ExecutedScript> getScriptNameExecutedScriptMap() {
        Map<String, ExecutedScript> scriptNameAlreadyExecutedScriptMap = new HashMap<String, ExecutedScript>();
        for (ExecutedScript executedScript : executedScriptInfoSource.getExecutedScripts()) {
            scriptNameAlreadyExecutedScriptMap.put(executedScript.getScript().getFileName(), executedScript);
        }
        return scriptNameAlreadyExecutedScriptMap;
    }

    /**
     * @return All scripts, as a map from scriptName => Script
     */
    protected Map<String, Script> getScriptNameScriptMap() {
        Map<String, Script> scriptNameScriptMap = new HashMap<String, Script>();
        for (Script script : scriptRepository.getAllScripts()) {
            scriptNameScriptMap.put(script.getFileName(), script);
        }
        return scriptNameScriptMap;
    }


    protected Map<String, Set<Script>> getCheckSumScriptMap() {
        Map<String, Set<Script>> checkSumScriptMap = new HashMap<String, Set<Script>>();
        for (Script script : scriptRepository.getAllScripts()) {
            Set<Script> scriptsWithCheckSum = checkSumScriptMap.get(script.getCheckSum());
            if (scriptsWithCheckSum == null) {
                scriptsWithCheckSum = new HashSet<Script>();
                checkSumScriptMap.put(script.getCheckSum(), scriptsWithCheckSum);
            }
            scriptsWithCheckSum.add(script);
        }
        return checkSumScriptMap;
    }


    protected void addRegularScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        regularlyAddedOrModifiedScripts.add(new ScriptUpdate(scriptUpdateType, script));
    }


    protected void addIrregularScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        irregularScriptUpdates.add(new ScriptUpdate(scriptUpdateType, script));
    }


    protected void addIrregularScriptUpdate(ScriptUpdateType scriptUpdateType, Script script1, Script script2) {
        irregularScriptUpdates.add(new ScriptUpdate(scriptUpdateType, script1, script2));
    }


    private void addRepeatableScriptDeletion(Script script) {
        regularlyDeletedRepeatableScripts.add(new ScriptUpdate(REPEATABLE_SCRIPT_DELETED, script));
    }


    protected void addRegularlyAddedPatchScript(ScriptUpdateType scriptUpdateType, Script script) {
        regularlyAddedPatchScripts.add(new ScriptUpdate(scriptUpdateType, script));
    }


    protected void addPostprocessingScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        regularPostprocessingScriptUpdates.add(new ScriptUpdate(scriptUpdateType, script));
    }


    protected void addPostprocessingScriptUpdate(ScriptUpdateType scriptUpdateType, Script originalScript, Script renamedToScript) {
        regularPostprocessingScriptUpdates.add(new ScriptUpdate(scriptUpdateType, originalScript, renamedToScript));
    }


    protected void addRegularScriptRename(ScriptUpdateType scriptUpdateType, Script originalScript, Script renamedScript) {
        regularlyRenamedScripts.add(new ScriptUpdate(scriptUpdateType, originalScript, renamedScript));
    }

    protected Script getExecutedScriptWithHighestScriptIndex(Set<ExecutedScript> executedScripts) {
        Script result = null;
        for (ExecutedScript executedScript : executedScripts) {
            if (executedScript.getScript().isIncremental() && (result == null  || executedScript.getScript().compareTo(result) > 0)) {
                result = executedScript.getScript();
            }
        }
        return result;
    }

}
