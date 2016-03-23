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
import org.junit.Before;
import org.junit.Test;

import static org.dbmaintain.util.CollectionUtils.asSet;
import static org.dbmaintain.util.CollectionUtils.asSortedSet;
import static org.dbmaintain.util.TestUtils.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 * @since 28-dec-2008
 */
public class ScriptRepositoryTest {

    Script indexed1, repeatable1, postProcessing1, indexed2, repeatable2, postProcessing2, duplicateIndex, preProcessing1, preProcessing2;
    ScriptLocation scriptLocation1;
    ScriptLocation scriptLocation2;

    @Before
    public void init() {
        indexed1 = createScript("01_indexed1.sql");
        indexed2 = createScript("02_indexed2.sql");
        duplicateIndex = createScript("01_duplicateIndex.sql");
        repeatable1 = createScript("repeatable1.sql");
        repeatable2 = createScript("repeatable2.sql");
        postProcessing1 = createScript("postprocessing/01_post1.sql");
        postProcessing2 = createScript("postprocessing/02_post2.sql");
        preProcessing1 = createScript("preprocessing/01_pre1.sql");
        preProcessing2 = createScript("preprocessing/02_pre2.sql");


        scriptLocation1 = createArchiveScriptLocation(asSortedSet(indexed1, repeatable1, postProcessing1, preProcessing1), null);
        scriptLocation2 = createArchiveScriptLocation(asSortedSet(indexed2, repeatable2, postProcessing2, preProcessing2), null);
    }

    @Test
    public void getScripts() {
        ScriptRepository scriptRepository = new ScriptRepository(asSet(scriptLocation1, scriptLocation2), getTrivialQualifierEvaluator());

        assertEquals(asSortedSet(indexed1, indexed2), scriptRepository.getIndexedScripts());
        assertEquals(asSortedSet(repeatable1, repeatable2), scriptRepository.getRepeatableScripts());
        assertEquals(asSortedSet(postProcessing1, postProcessing2), scriptRepository.getPostProcessingScripts());
    }

    @Test(expected = DbMaintainException.class)
    public void errorInCaseOfDuplicateScript() {
        ScriptLocation location = createArchiveScriptLocation(asSortedSet(indexed2, repeatable1, postProcessing2), null);
        new ScriptRepository(asSet(scriptLocation1, location), getTrivialQualifierEvaluator());
    }

    @Test(expected = DbMaintainException.class)
    public void errorInCaseOfDuplicateIndex() {
        ScriptLocation location = createArchiveScriptLocation(asSortedSet(indexed2, duplicateIndex, repeatable2, postProcessing2), null);
        new ScriptRepository(asSet(scriptLocation1, location), getTrivialQualifierEvaluator());
    }

}
