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

import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.dbmaintain.util.TestUtils.*;

/**
 * Tests for checking whether 2 scripts have an equal content => checks for timestamp and check sums
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ScriptIsContentEqualTest {

    @Test
    public void testIsScriptContentEqualTo() {
        Script script = createScriptWithContent("fileName", "script content");

        Script sameScriptWithoutContent = createScriptWithCheckSum("fileName", script.getCheckSum());
        assertEqualScriptContent(script, sameScriptWithoutContent, true);
        assertEqualScriptContent(script, sameScriptWithoutContent, false);

        Script scriptWithDifferentModificationDate = createScriptWithModificationDateAndCheckSum("fileName", 1L, script.getCheckSum());
        assertEqualScriptContent(script, scriptWithDifferentModificationDate, true);
        assertEqualScriptContent(script, scriptWithDifferentModificationDate, false);

        Script scriptWithDifferentChecksum = createScriptWithCheckSum("fileName", "xxx");
        assertEqualScriptContent(script, scriptWithDifferentChecksum, true);
        assertDifferentScriptContent(script, scriptWithDifferentChecksum, false);

        Script scriptWithDifferentChecksumAndModificationDate = createScriptWithModificationDateAndCheckSum("fileName", 1L, "xxx");
        assertDifferentScriptContent(script, scriptWithDifferentChecksumAndModificationDate, true);
        assertDifferentScriptContent(script, scriptWithDifferentChecksumAndModificationDate, false);
    }


    private void assertEqualScriptContent(Script script1, Script script2, boolean useLastModificationDates) {
        assertTrue(script1.isScriptContentEqualTo(script2, useLastModificationDates));
    }

    private void assertDifferentScriptContent(Script script1, Script script2, boolean useLastModificationDates) {
        assertFalse(script1.isScriptContentEqualTo(script2, useLastModificationDates));
    }
}
