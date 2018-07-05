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
package org.dbmaintain.script.repository;

import org.dbmaintain.script.Script;
import org.dbmaintain.script.ScriptFactory;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.dbmaintain.util.CollectionUtils.asSortedSet;
import static org.dbmaintain.util.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
class ScriptRepositoryBaselineRevisionTest {

    /* Tested object */
    private ScriptRepository scriptRepository;


    @Test
    void someScriptsFiltered() {
        scriptRepository = createScriptRepository(new ScriptIndexes("1.2"));

        SortedSet<Script> result = scriptRepository.getIndexedScripts();
        List<String> filenames = result.stream().map(Script::getFileName).collect(Collectors.toList());

        assertEquals(asList("1_folder/2_script.sql", "2_folder/1_script.sql"), filenames);
    }


    @Test
    void allScriptsFiltered() {
        scriptRepository = createScriptRepository(new ScriptIndexes("999"));

        SortedSet<Script> result = scriptRepository.getIndexedScripts();
        assertTrue(result.isEmpty());
    }

    @Test
    void noScriptsFiltered() {
        scriptRepository = createScriptRepository(new ScriptIndexes("1.0"));

        SortedSet<Script> result = scriptRepository.getIndexedScripts();
        List<String> filenames = result.stream().map(Script::getFileName).collect(Collectors.toList());

        assertEquals(asList("1_folder/1_script.sql", "1_folder/2_script.sql", "2_folder/1_script.sql"), filenames);
    }

    @Test
    void repeatableScriptsNotFiltered() {
        scriptRepository = createScriptRepository(new ScriptIndexes("1.0"));

        SortedSet<Script> result = scriptRepository.getRepeatableScripts();
        List<String> filenames = result.stream().map(Script::getFileName).collect(Collectors.toList());

        assertEquals(Collections.singletonList("repeatable/script.sql"), filenames);
    }

    @Test
    void preProcessingScriptsNotFiltered() {
    	scriptRepository = createScriptRepository(new ScriptIndexes("1.0"));

    	SortedSet<Script> result = scriptRepository.getPreProcessingScripts();
        List<String> filenames = result.stream().map(Script::getFileName).collect(Collectors.toList());

        assertEquals(Collections.singletonList("preprocessing/script.sql"), filenames);
    }

    @Test
    void postProcessingScriptsNotFiltered() {
        scriptRepository = createScriptRepository(new ScriptIndexes("1.0"));

        SortedSet<Script> result = scriptRepository.getPostProcessingScripts();
        List<String> filenames = result.stream().map(Script::getFileName).collect(Collectors.toList());

        assertEquals(Collections.singletonList("postprocessing/script.sql"), filenames);
    }


    private ScriptRepository createScriptRepository(ScriptIndexes baseLineRevision) {
        ScriptFactory scriptFactory = createScriptFactory(baseLineRevision);
        Script script11 = scriptFactory.createScriptWithoutContent("1_folder/1_script.sql", 0L, "checksum");
        Script script12 = scriptFactory.createScriptWithoutContent("1_folder/2_script.sql", 0L, "checksum");
        Script script21 = scriptFactory.createScriptWithoutContent("2_folder/1_script.sql", 0L, "checksum");
        Script repeatableScript = scriptFactory.createScriptWithoutContent("repeatable/script.sql", 0L, "checksum");
        Script preProcessingScript = scriptFactory.createScriptWithoutContent("preprocessing/script.sql", 0L, "checksum");
        Script postProcessingScript = scriptFactory.createScriptWithoutContent("postprocessing/script.sql", 0L, "checksum");
        SortedSet<Script> scripts = asSortedSet(script11, script12, script21, repeatableScript, preProcessingScript, postProcessingScript);

        ScriptLocation scriptLocation = createArchiveScriptLocation(scripts, baseLineRevision);
        return new ScriptRepository(Collections.singleton(scriptLocation), getTrivialQualifierEvaluator());
    }


}
