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
import org.dbmaintain.util.DbMaintainException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.dbmaintain.util.TestUtils.createArchiveScriptLocation;
import static org.dbmaintain.util.TestUtils.createScript;
import static org.dbmaintain.util.TestUtils.getTrivialQualifierEvaluator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 28-dec-2008
 */
class ScriptRepositoryTest {

    private Script indexed1;
    private Script repeatable1;
    private Script postProcessing1;
    private Script indexed2;
    private Script repeatable2;
    private Script postProcessing2;
    private Script duplicateIndex;
    private Script preProcessing1;
    private Script preProcessing2;
    private ScriptLocation scriptLocation1;
    private ScriptLocation scriptLocation2;

    @BeforeEach
    void init() {
        indexed1 = createScript("01_indexed1.sql");
        indexed2 = createScript("02_indexed2.sql");
        duplicateIndex = createScript("01_duplicateIndex.sql");
        repeatable1 = createScript("repeatable1.sql");
        repeatable2 = createScript("repeatable2.sql");
        postProcessing1 = createScript("postprocessing/01_post1.sql");
        postProcessing2 = createScript("postprocessing/02_post2.sql");
        preProcessing1 = createScript("preprocessing/01_pre1.sql");
        preProcessing2 = createScript("preprocessing/02_pre2.sql");

        scriptLocation1 = createArchiveScriptLocation(
                Stream.of(indexed1, repeatable1, postProcessing1, preProcessing1).collect(Collectors.toCollection(TreeSet::new)), null);
        scriptLocation2 = createArchiveScriptLocation(
                Stream.of(indexed2, repeatable2, postProcessing2, preProcessing2).collect(Collectors.toCollection(TreeSet::new)), null);
    }

    @Test
    void getScripts() {
        ScriptRepository scriptRepository = new ScriptRepository(
                Stream.of(scriptLocation1, scriptLocation2).collect(Collectors.toSet()), getTrivialQualifierEvaluator());

        assertEquals(Stream.of(indexed1, indexed2).collect(Collectors.toCollection(TreeSet::new)), scriptRepository.getIndexedScripts());
        assertEquals(Stream.of(repeatable1, repeatable2).collect(Collectors.toCollection(TreeSet::new)),
                scriptRepository.getRepeatableScripts());
        assertEquals(Stream.of(postProcessing1, postProcessing2).collect(Collectors.toCollection(TreeSet::new)),
                scriptRepository.getPostProcessingScripts());
    }

    @Test
    void errorInCaseOfDuplicateScript() {
        ScriptLocation location = createArchiveScriptLocation(
                Stream.of(indexed2, repeatable1, postProcessing2).collect(Collectors.toCollection(TreeSet::new)), null);
        assertThrows(DbMaintainException.class,
                () -> new ScriptRepository(Stream.of(scriptLocation1, location).collect(Collectors.toSet()),
                        getTrivialQualifierEvaluator()));
    }

    @Test
    void errorInCaseOfDuplicateIndex() {
        ScriptLocation location = createArchiveScriptLocation(
                Stream.of(indexed2, duplicateIndex, repeatable2, postProcessing2).collect(Collectors.toCollection(TreeSet::new)), null);
        assertThrows(DbMaintainException.class,
                () -> new ScriptRepository(Stream.of(scriptLocation1, location).collect(Collectors.toSet()),
                        getTrivialQualifierEvaluator()));
    }

}
