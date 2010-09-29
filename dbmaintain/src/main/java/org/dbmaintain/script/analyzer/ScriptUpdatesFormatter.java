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

import org.apache.commons.lang.StringUtils;

import java.util.SortedSet;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 6-feb-2009
 */
public class ScriptUpdatesFormatter {


    /**
     * @param scriptUpdates The script updates, not null
     * @return An printable overview of the regular script updates
     */
    public String formatScriptUpdates(SortedSet<ScriptUpdate> scriptUpdates) {
        StringBuilder formattedUpdates = new StringBuilder();
        int index = 0;
        for (ScriptUpdate scriptUpdate : scriptUpdates) {
            formattedUpdates.append("  ").append(++index).append(". ").append(StringUtils.capitalize(formatScriptUpdate(scriptUpdate))).append("\n");
        }
        return formattedUpdates.toString();
    }


    /**
     * @param scriptUpdate The script update to format, not null
     * @return A printable view of the given script update
     */
    public String formatScriptUpdate(ScriptUpdate scriptUpdate) {
        switch (scriptUpdate.getType()) {
            case HIGHER_INDEX_SCRIPT_ADDED:
                return "newly added indexed script: " + scriptUpdate.getScript().getFileName();
            case REPEATABLE_SCRIPT_ADDED:
                return "newly added repeatable script: " + scriptUpdate.getScript().getFileName();
            case REPEATABLE_SCRIPT_UPDATED:
                return "updated repeatable script: " + scriptUpdate.getScript().getFileName();
            case REPEATABLE_SCRIPT_DELETED:
                return "deleted repeatable script: " + scriptUpdate.getScript().getFileName();
            case POSTPROCESSING_SCRIPT_ADDED:
                return "newly added postprocessing script: " + scriptUpdate.getScript().getFileName();
            case POSTPROCESSING_SCRIPT_UPDATED:
                return "updated postprocessing script: " + scriptUpdate.getScript().getFileName();
            case POSTPROCESSING_SCRIPT_DELETED:
                return "deleted postprocessing script: " + scriptUpdate.getScript().getFileName();
            case POSTPROCESSING_SCRIPT_FAILURE_RERUN:
                return "re-run of failed postprocessing script: " + scriptUpdate.getScript().getFileName();
            case INDEXED_SCRIPT_UPDATED:
                return "updated indexed script: " + scriptUpdate.getScript().getFileName();
            case INDEXED_SCRIPT_DELETED:
                return "deleted indexed script: " + scriptUpdate.getScript().getFileName();
            case LOWER_INDEX_NON_PATCH_SCRIPT_ADDED:
                return "newly added script with a lower index: "
                        + scriptUpdate.getScript().getFileName();
            case LOWER_INDEX_PATCH_SCRIPT_ADDED:
                return "newly added patch script with a lower index: "
                        + scriptUpdate.getScript().getFileName();
            case INDEXED_SCRIPT_RENAMED:
                return "renamed indexed script " + scriptUpdate.getScript() + " into " + scriptUpdate.getRenamedToScript()
                        + ", without changing the sequence of the scripts";
            case INDEXED_SCRIPT_RENAMED_SCRIPT_SEQUENCE_CHANGED:
                return "renamed indexed script " + scriptUpdate.getScript() + " into " + scriptUpdate.getRenamedToScript()
                        + ", which changes the sequence of the scripts";
            case REPEATABLE_SCRIPT_RENAMED:
                return "renamed repeatable script " + scriptUpdate.getScript() + " into " + scriptUpdate.getRenamedToScript();
            case POSTPROCESSING_SCRIPT_RENAMED:
                return "renamed postprocessing script " + scriptUpdate.getScript() + " into " + scriptUpdate.getRenamedToScript();
        }
        throw new IllegalArgumentException("Invalid script update type " + scriptUpdate.getType());
    }
}
