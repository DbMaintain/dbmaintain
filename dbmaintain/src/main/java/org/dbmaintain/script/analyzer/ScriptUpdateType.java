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
package org.dbmaintain.script.analyzer;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 24-dec-2008
 */
public enum ScriptUpdateType {

    /**
     * 'Regular' script updates: these are always allowed
     */
    HIGHER_INDEX_SCRIPT_ADDED, REPEATABLE_SCRIPT_ADDED, REPEATABLE_SCRIPT_UPDATED,

    /**
     * Special case: deletion of a repeatable script: this is allowed, thought might introduce problems
     */
    REPEATABLE_SCRIPT_DELETED,

    /**
     * Preprocessing script updates: these are also always allowed
     */
    PREPROCESSING_SCRIPT_ADDED, PREPROCESSING_SCRIPT_UPDATED, PREPROCESSING_SCRIPT_DELETED, PREPROCESSING_SCRIPT_FAILURE_RERUN,

    /**
     * Postprocessing script updates: these are also always allowed
     */
    POSTPROCESSING_SCRIPT_ADDED, POSTPROCESSING_SCRIPT_UPDATED, POSTPROCESSING_SCRIPT_DELETED, POSTPROCESSING_SCRIPT_FAILURE_RERUN,

    /**
     * Irregular script updates: these cause an exception or trigger a recreation from scratch
     */
    INDEXED_SCRIPT_UPDATED, INDEXED_SCRIPT_DELETED, LOWER_INDEX_NON_PATCH_SCRIPT_ADDED, INDEXED_SCRIPT_RENAMED_SCRIPT_SEQUENCE_CHANGED,

    /**
     * Script update that is only allowed if the 'patch.allowOutOfSequenceExecution' option is enabled
     */
    LOWER_INDEX_PATCH_SCRIPT_ADDED,

    /**
     * Regular script renames.
     */
    INDEXED_SCRIPT_RENAMED, REPEATABLE_SCRIPT_RENAMED, PREPROCESSING_SCRIPT_RENAMED, POSTPROCESSING_SCRIPT_RENAMED;

}
