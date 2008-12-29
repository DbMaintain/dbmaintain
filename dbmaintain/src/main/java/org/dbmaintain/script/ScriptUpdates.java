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

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 24-dec-2008
 */
public class ScriptUpdates {

    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> regularScriptUpdates;

    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> irregularScriptUpdates;

    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> repeatableScriptDeletions;

    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> regularPatchScriptUpdates;

    private Map<ScriptUpdateType, SortedSet<ScriptUpdate>> postprocessingScriptUpdates;


    protected ScriptUpdates(Map<ScriptUpdateType, SortedSet<ScriptUpdate>> regularScriptUpdates, Map<ScriptUpdateType, SortedSet<ScriptUpdate>> irregularScriptUpdates,
                         Map<ScriptUpdateType, SortedSet<ScriptUpdate>> repeatableScriptDeletions, Map<ScriptUpdateType, SortedSet<ScriptUpdate>> regularPatchScriptUpdates,
                         Map<ScriptUpdateType, SortedSet<ScriptUpdate>> postprocessingScriptUpdates) {
        this.regularScriptUpdates = regularScriptUpdates;
        this.irregularScriptUpdates = irregularScriptUpdates;
        this.repeatableScriptDeletions = repeatableScriptDeletions;
        this.regularPatchScriptUpdates = regularPatchScriptUpdates;
        this.postprocessingScriptUpdates = postprocessingScriptUpdates;
    }


    public SortedSet<ScriptUpdate> getRegularScriptUpdates(ScriptUpdateType scriptUpdateType) {
        return regularScriptUpdates.get(scriptUpdateType);
    }


    public SortedSet<ScriptUpdate> getRegularScriptUpdates() {
        SortedSet<ScriptUpdate> result = new TreeSet<ScriptUpdate>();
        for (ScriptUpdateType regularScriptUpdateType : ScriptUpdateType.getRegularScriptUpdateTypes()) {
            result.addAll(regularScriptUpdates.get(regularScriptUpdateType));
        }
        return result;
    }


    public boolean hasIrregularScriptUpdates() {
        for (ScriptUpdateType scriptUpdateType : ScriptUpdateType.getIrregularScriptUpdateTypes()) {
            if (irregularScriptUpdates.get(scriptUpdateType).size() > 0) {
                return true;
            }
        }
        return false;
    }


    public SortedSet<ScriptUpdate> getIrregularScriptUpdates(ScriptUpdateType scriptUpdateType) {
        return irregularScriptUpdates.get(scriptUpdateType);
    }


    public SortedSet<ScriptUpdate> getIrregularScriptUpdates() {
        SortedSet<ScriptUpdate> result = new TreeSet<ScriptUpdate>();
        for (ScriptUpdateType irregularScriptUpdateType : ScriptUpdateType.getIrregularScriptUpdateTypes()) {
            result.addAll(irregularScriptUpdates.get(irregularScriptUpdateType));
        }
        return result;
    }


    public SortedSet<ScriptUpdate> getRepeatableScriptDeletions() {
        SortedSet<ScriptUpdate> result = new TreeSet<ScriptUpdate>();
        for (ScriptUpdateType irregularScriptUpdateType : ScriptUpdateType.getRepeatableScriptDeletionTypes()) {
            result.addAll(repeatableScriptDeletions.get(irregularScriptUpdateType));
        }
        return result;
    }


    public SortedSet<ScriptUpdate> getRegularPatchScriptUpdates(ScriptUpdateType scriptUpdateType) {
        return regularPatchScriptUpdates.get(scriptUpdateType);
    }


    public SortedSet<ScriptUpdate> getRegularPatchScriptUpdates() {
        SortedSet<ScriptUpdate> result = new TreeSet<ScriptUpdate>();
        for (ScriptUpdateType patchScriptUpdateType : ScriptUpdateType.getPatchScriptUpdateTypes()) {
            result.addAll(regularPatchScriptUpdates.get(patchScriptUpdateType));
        }
        return result;
    }


    protected SortedSet<ScriptUpdate> getPostprocessingScriptUpdates() {
        SortedSet<ScriptUpdate> result = new TreeSet<ScriptUpdate>();
        for (ScriptUpdateType postProcessingScriptUpdateType : ScriptUpdateType.getPostprocessingScriptUpdateTypes()) {
            result.addAll(postprocessingScriptUpdates.get(postProcessingScriptUpdateType));
        }
        return result;
    }

    
    public SortedSet<ScriptUpdate> getPostprocessingScriptUpdates(ScriptUpdateType scriptUpdateType) {
        return postprocessingScriptUpdates.get(scriptUpdateType);
    }


    public boolean hasUpdatesOtherThanRepeatableScriptDeletions() {
        return getRegularScriptUpdates().size() != 0 || getIrregularScriptUpdates().size() != 0 ||
                getRegularPatchScriptUpdates().size() != 0 || getPostprocessingScriptUpdates().size() != 0;
    }
}
