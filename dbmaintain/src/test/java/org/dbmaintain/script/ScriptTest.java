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
package org.dbmaintain.script;

import org.dbmaintain.util.DbMaintainException;
import org.junit.jupiter.api.Test;

import static org.dbmaintain.util.TestUtils.createScript;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the script class
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
class ScriptTest {

    @Test
    void testIsIncremental_incremental() {
        Script script = createScript("incremental/02_sprint1/01_incrementalScript.sql");
        assertTrue(script.isIncremental());
    }

    @Test
    void testIsIncremental_repeatable() {
        Script script = createScript("repeatable/repeatableScript.sql");
        assertTrue(script.isRepeatable());
    }

    /**
     * Verifies that an exception is thrown when a repeatable script is located inside an indexed folder.
     */
    @Test
    void testIsIncremental_repeatableScriptInsideIndexedFolder() {
        assertThrows(DbMaintainException.class, () -> createScript("incremental/02_script1/repeatableScript.sql"));
    }

    @Test
    void testOrder() {
    	Script preprocessing1 = createScript("preprocessing/01_pre.sql");
        Script preprocessing2 = createScript("preprocessing/noindex.sql");
        Script incremental1 = createScript("01_x/01_x.sql");
        Script incremental2 = createScript("01_x/02_x.sql");
        Script incremental3 = createScript("02_x/01_x.sql");
        Script incremental4 = createScript("02_y/01_y.sql");
        Script incremental5 = createScript("noindex/01_x.sql");
        Script postprocessing1 = createScript("postprocessing/01_x.sql");
        Script postprocessing2 = createScript("postprocessing/02_x.sql");
        Script postprocessing3 = createScript("postprocessing/noindex.sql");


        assertSequence(preprocessing1, preprocessing2, incremental1, incremental2, incremental3, incremental4, incremental5,
                postprocessing1, postprocessing2, postprocessing3);
    }

    private void assertSequence(Script... scripts) {
        for (int i = 0; i < scripts.length - 1; i++) {
            Script script1 = scripts[i];
            Script script2 = scripts[i + 1];
            assertEquals(-1, script1.compareTo(script2), "Expected script " + script1 + " to come before " + script2 + " but it doesn't");
        }
    }
}
