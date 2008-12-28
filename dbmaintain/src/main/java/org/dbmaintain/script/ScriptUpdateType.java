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

import java.util.EnumSet;
import java.util.Set;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 24-dec-2008
 */
public enum ScriptUpdateType {

    /**
     * 'Regular' script updates: these are always allowed
     */
    HIGHER_INDEX_SCRIPT_ADDED, REPEATABLE_SCRIPT_ADDED, REPEATABLE_SCRIPT_UPDATED, REPEATABLE_SCRIPT_DELETED,

    /**
     * Postprocessing script updates: these are also always allowed
     */
    POSTPROCESSING_SCRIPT_ADDED, POSTPROCESSING_SCRIPT_UPDATED, POSTPROCESSING_SCRIPT_DELETED,

    /**
     * 'Irregular' script updates: these cause an exception or trigger a recreation from scratch
     */
    INDEXED_SCRIPT_UPDATED, INDEXED_SCRIPT_DELETED, LOWER_INDEX_NON_PATCH_SCRIPT_ADDED,

    /**
     * Script update that is only allowed if the 'patch.allowOutOfSequenceExecution' option is enabled
     */
    LOWER_INDEX_PATCH_SCRIPT_ADDED;


    public static Set<ScriptUpdateType> getRegularScriptUpdateTypes() {
        return EnumSet.of(HIGHER_INDEX_SCRIPT_ADDED, REPEATABLE_SCRIPT_ADDED, REPEATABLE_SCRIPT_UPDATED);
    }

    public static Set<ScriptUpdateType> getPostprocessingScriptUpdateTypes() {
        return EnumSet.of(POSTPROCESSING_SCRIPT_ADDED, POSTPROCESSING_SCRIPT_UPDATED, POSTPROCESSING_SCRIPT_DELETED);
    }

    public static Set<ScriptUpdateType> getIrregularScriptUpdateTypes() {
        return EnumSet.of(INDEXED_SCRIPT_UPDATED, INDEXED_SCRIPT_DELETED, LOWER_INDEX_PATCH_SCRIPT_ADDED, LOWER_INDEX_NON_PATCH_SCRIPT_ADDED);
    }

    public static Set<ScriptUpdateType> getPatchScriptUpdateTypes() {
        return EnumSet.of(LOWER_INDEX_PATCH_SCRIPT_ADDED);
    }
}
