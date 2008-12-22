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

import static junit.framework.Assert.*;

import org.dbmaintain.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.util.DbMaintainException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for the script class
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ScriptTest {
    
    @Test
    public void testIsIncremental_incremental() {
        Script script = createScript("incremental/02_sprint1/01_incrementalScript.sql");
        assertTrue(script.isIncremental());
    }
    
    @Test
    public void testIsIncremental_repeatable() {
        Script script = createScript("repeatable/repeatableScript.sql");
        assertTrue(script.isRepeatable());
    }
    
    /**
     * Verifies that an exception is thrown when a repeatable script is located inside an indexed folder. 
     */
    @Test(expected = DbMaintainException.class)
    public void testIsIncremental_repeatableScriptInsideIndexedFolder() {
        createScript("incremental/02_script1/repeatableScript.sql");
    }

    /**
     * Tests for a script name with a 'fix' in the name.
     */
    @Test
    public void testIsPatchScript_caseIgnored() {
        Script script = createScript("incremental/02_sprint2/03_#patch_addUser.sql");
        assertTrue(script.isPatchScript());
    }


    /**
     * Tests for a script name with a 'fix' in the folder name.
     */
    @Test
    public void testIsPatchScript_patchInFolderName() {
        Script script = createScript("incremental/02_#patch_sprint2/03_addUser.sql");
        assertTrue(script.isPatchScript());
    }


    /**
     * Tests for a script name with a 'fix' in the name but no index.
     */
    @Test
    public void testIsPatchScript_patchWithoutIndex() {
        Script script = createScript("#patch_incremental/02_sprint2/03_addUser.sql");
        assertTrue(script.isPatchScript());
    }


    /**
     * Tests for a script name with no 'patch' in the name.
     */
    @Test
    public void testIsPatchScript_noPatch() {
        Script script = createScript("incremental/02_sprint2/03_addUser.sql");
        assertFalse(script.isPatchScript());
    }


    /**
     * The patch indicator should be case-insensitive.
     */
    @Test
    public void testIsPatchScript_caseInsensitive() {
        Script script = createScript("incremental/02_sprint2/03_#PaTcH_addUser.sql");
        assertTrue(script.isPatchScript());
    }


    @Test
    public void testNoTargetDatabase() {
        Script script = createScript("incremental/02_sprint2/03_addUser.sql");
        assertNull(script.getTargetDatabaseName());
        assertEquals(new ScriptIndexes(Arrays.asList(null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testTargetDatabaseNameInFileName() {
        Script script = createScript("incremental/02_sprint2/03_@otherdb_addUser.sql");
        assertEquals("otherdb", script.getTargetDatabaseName());
        assertEquals(new ScriptIndexes(Arrays.asList(null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testGetTargetDatabaseName_inDirName() {
        Script script = createScript("incremental/02_@otherdb_sprint2/03_addUser.sql");
        assertEquals("otherdb", script.getTargetDatabaseName());
        assertEquals(new ScriptIndexes(Arrays.asList(null, 2L, 3L)), script.getVersion());
    }


    @Test
    public void testGetTargetDatabaseName_inDirAndFileName() {
        Script script = createScript("incremental/02_@otherdb_sprint2/03_@thisdb_addUser.sql");
        assertEquals("thisdb", script.getTargetDatabaseName());
        assertEquals(new ScriptIndexes(Arrays.asList(null, 2L, 3L)), script.getVersion());
    }
    
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

    @Test
    public void testOrder() {
        Script incremental1 = createScript("01_x/01_x.sql");
        Script incremental2 = createScript("01_x/02_x.sql");
        Script incremental3 = createScript("02_x/01_x.sql");
        Script incremental4 = createScript("02_y/01_y.sql");
        Script incremental5 = createScript("noindex/01_x.sql");
        Script postprocessing1 = createScript("postprocessing/01_x.sql");
        Script postprocessing2 = createScript("postprocessing/02_x.sql");
        Script postprocessing3 = createScript("postprocessing/noindex.sql");

        assertSequence(incremental1, incremental2, incremental3, incremental4, incremental5,
                postprocessing1, postprocessing2, postprocessing3);
    }

    private void assertSequence(Script... scripts) {
        for (int i = 0; i < scripts.length - 1; i++) {
            Script script1 = scripts[i];
            Script script2 = scripts[i + 1];
            assertEquals("Expected script " + script1 + " to come before " + script2 + " but it doesn't", -1, script1.compareTo(script2));
        }
    }

    private void assertEqualScriptContent(Script script1, Script script2, boolean useLastModificationDates) {
        assertTrue(script1.isScriptContentEqualTo(script2, useLastModificationDates));
    }

    private void assertDifferentScriptContent(Script script1, Script script2, boolean useLastModificationDates) {
        assertFalse(script1.isScriptContentEqualTo(script2, useLastModificationDates));
    }

    private Script createScript(String fileName) {
        return new Script(fileName, 10L, "xxx", "@", "#", Collections.singleton("PATCH"), "postprocessing");
    }

    private Script createScriptWithContent(String fileName, String scriptContent) {
        return new Script(fileName, 0L, new ScriptContentHandle.StringScriptContentHandle(scriptContent, "ISO-8859-1"), "@", "#", Collections.singleton("PATCH"), "postprocessing");
    }

    private Script createScriptWithCheckSum(String fileName, String checkSum) {
        return new Script(fileName, 0L, checkSum, "@", "#", Collections.singleton("PATCH"), "postprocessing");
    }

    private Script createScriptWithModificationDateAndCheckSum(String fileName, long fileLastModifiedAt, String checkSum) {
        return new Script(fileName, fileLastModifiedAt, checkSum, "@", "#", Collections.singleton("PATCH"), "postprocessing");
    }

}
