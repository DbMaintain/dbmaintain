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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 29-dec-2008
 */
public class ScriptUpdatesAnalyzer {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(ScriptUpdates.class);

    private ScriptRepository scriptRepository;
    private ExecutedScriptInfoSource executedScriptInfoSource;
    private boolean useScriptFileLastModificationDates;
    private boolean allowOutOfSequenceExecutionOfPatchScripts;

    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> regularScriptUpdates;
    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> irregularScriptUpdates;
    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> repeatableScriptDeletions;
    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> patchScriptUpdates;
    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> postprocessingScriptUpdates;

    public ScriptUpdatesAnalyzer(ScriptRepository scriptRepository, ExecutedScriptInfoSource executedScriptInfoSource,
                         boolean useScriptFileLastModificationDates, boolean allowOutOfSequenceExecutionOfPatchScripts) {
        this.scriptRepository = scriptRepository;
        this.executedScriptInfoSource = executedScriptInfoSource;
        this.useScriptFileLastModificationDates = useScriptFileLastModificationDates;
        this.allowOutOfSequenceExecutionOfPatchScripts = allowOutOfSequenceExecutionOfPatchScripts;
    }

    public ScriptUpdates calculateScriptUpdates() {
        regularScriptUpdates = initScriptUpdateMap(ScriptUpdateType.getRegularScriptUpdateTypes());
        irregularScriptUpdates = initScriptUpdateMap(ScriptUpdateType.getIrregularScriptUpdateTypes());
        repeatableScriptDeletions = initScriptUpdateMap(ScriptUpdateType.getRepeatableScriptDeletionTypes());
        patchScriptUpdates = initScriptUpdateMap(ScriptUpdateType.getPatchScriptUpdateTypes());
        postprocessingScriptUpdates = initScriptUpdateMap(ScriptUpdateType.getPostprocessingScriptUpdateTypes());

        Map<Script, ExecutedScript> alreadyExecutedScripts = getAlreadyExecutedScripts(executedScriptInfoSource);
        // Search for indexed scriptNames that have been executed but don't appear in the current indexed scriptNames anymore
        SortedSet<Script> allScripts = scriptRepository.getAllScripts();
        for (Script alreadyExecutedScript : alreadyExecutedScripts.keySet()) {
            if (!allScripts.contains(alreadyExecutedScript)) {
                if (alreadyExecutedScript.isIncremental()) {
                    addIrregularScriptUpdate(INDEXED_SCRIPT_DELETED, alreadyExecutedScript);
                } else if (alreadyExecutedScript.isRepeatable()) {
                    addRepeatableScriptDeletion(alreadyExecutedScript);
                } else if (alreadyExecutedScript.isPostProcessingScript()) {
                    addPostprocessingScriptUpdate(POSTPROCESSING_SCRIPT_DELETED, alreadyExecutedScript);
                }
            }
        }

        // Search for indexed scriptNames whose version < the current version, which are new or whose contents have changed
        Script scriptWithHighestScriptIndex = getExecutedScriptWithHighestScriptIndex(executedScriptInfoSource.getExecutedScripts());
        for (Script indexedScript : scriptRepository.getIndexedScripts()) {
            if (!alreadyExecutedScripts.containsKey(indexedScript)) {
                if (scriptWithHighestScriptIndex == null || indexedScript.compareTo(scriptWithHighestScriptIndex) > 0) {
                    addRegularScriptUpdate(HIGHER_INDEX_SCRIPT_ADDED, indexedScript);
                } else {
                    if (indexedScript.isPatchScript()) {
                        if (allowOutOfSequenceExecutionOfPatchScripts) {
                            addPatchScriptUpdate(LOWER_INDEX_PATCH_SCRIPT_ADDED, indexedScript);
                        } else {
                            addIrregularScriptUpdate(LOWER_INDEX_PATCH_SCRIPT_ADDED, indexedScript);
                        }
                    } else {
                        addIrregularScriptUpdate(LOWER_INDEX_NON_PATCH_SCRIPT_ADDED, indexedScript);
                    }
                }
            } else {
                if (!alreadyExecutedScripts.get(indexedScript).getScript().isScriptContentEqualTo(indexedScript, useScriptFileLastModificationDates)) {
                    addIrregularScriptUpdate(INDEXED_SCRIPT_UPDATED, indexedScript);
                }
            }
        }

        for (Script repeatableScript : scriptRepository.getRepeatableScripts()) {
            if (!alreadyExecutedScripts.containsKey(repeatableScript)) {
                addRegularScriptUpdate(REPEATABLE_SCRIPT_ADDED, repeatableScript);
            } else {
                if (!alreadyExecutedScripts.get(repeatableScript).getScript().isScriptContentEqualTo(repeatableScript, useScriptFileLastModificationDates)) {
                    addRegularScriptUpdate(REPEATABLE_SCRIPT_UPDATED, repeatableScript);
                }
            }
        }

        for (Script postprocessingScript : scriptRepository.getPostProcessingScripts()) {
            if (!alreadyExecutedScripts.containsKey(postprocessingScript)) {
                addPostprocessingScriptUpdate(POSTPROCESSING_SCRIPT_ADDED, postprocessingScript);
            } else {
                if (!alreadyExecutedScripts.get(postprocessingScript).getScript().isScriptContentEqualTo(postprocessingScript, useScriptFileLastModificationDates)) {
                    addPostprocessingScriptUpdate(POSTPROCESSING_SCRIPT_UPDATED, postprocessingScript);
                }
            }
        }

        return new ScriptUpdates(regularScriptUpdates, irregularScriptUpdates, repeatableScriptDeletions, patchScriptUpdates, postprocessingScriptUpdates);
    }

    protected Map<ScriptUpdateType, SortedSet<ScriptUpdate>> initScriptUpdateMap(Set<ScriptUpdateType> scriptUpdateTypes) {
        Map<ScriptUpdateType, SortedSet<ScriptUpdate>> result = new EnumMap<ScriptUpdateType, SortedSet<ScriptUpdate>>(ScriptUpdateType.class);
        for (ScriptUpdateType scriptUpdateType : scriptUpdateTypes) {
            result.put(scriptUpdateType, new TreeSet<ScriptUpdate>());
        }
        return result;
    }

    /**
     * @return The already executed scripts, as a map from Script => ExecutedScript
     */
    protected Map<Script, ExecutedScript> getAlreadyExecutedScripts(ExecutedScriptInfoSource executedScriptInfoSource) {
        Map<Script, ExecutedScript> alreadyExecutedScripts = new HashMap<Script, ExecutedScript>();
        for (ExecutedScript executedScript : executedScriptInfoSource.getExecutedScripts()) {
            alreadyExecutedScripts.put(executedScript.getScript(), executedScript);
        }
        return alreadyExecutedScripts;
    }


    protected void addRegularScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        regularScriptUpdates.get(scriptUpdateType).add(new ScriptUpdate(scriptUpdateType, script));
    }


    protected void addIrregularScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        irregularScriptUpdates.get(scriptUpdateType).add(new ScriptUpdate(scriptUpdateType, script));
    }


    private void addRepeatableScriptDeletion(Script script) {
        repeatableScriptDeletions.get(REPEATABLE_SCRIPT_DELETED).add(new ScriptUpdate(REPEATABLE_SCRIPT_DELETED, script));
    }


    protected void addPatchScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        patchScriptUpdates.get(scriptUpdateType).add(new ScriptUpdate(scriptUpdateType, script));
    }


    protected void addPostprocessingScriptUpdate(ScriptUpdateType scriptUpdateType, Script script) {
        postprocessingScriptUpdates.get(scriptUpdateType).add(new ScriptUpdate(scriptUpdateType, script));
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
