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
 * todo javadoc
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
        Script script = new Script("fileName", 0L, new ScriptContentHandle.StringScriptContentHandle("script content", "ISO-8859-1"), "@", "#", Collections.singleton("PATCH"), "postprocessing");

        Script sameScriptWithoutContent = new Script("fileName", 0L, script.getCheckSum(), Collections.singleton("PATCH"), "@", "#", "postprocessing");
        assertTrue(script.isScriptContentEqualTo(sameScriptWithoutContent, true));
        assertTrue(script.isScriptContentEqualTo(sameScriptWithoutContent, false));

        Script scriptWithDifferentModificationDate = new Script("fileName", 1L, script.getCheckSum(), Collections.singleton("PATCH"), "@", "#", "postprocessing");
        assertTrue(script.isScriptContentEqualTo(scriptWithDifferentModificationDate, true));
        assertTrue(script.isScriptContentEqualTo(scriptWithDifferentModificationDate, false));

        Script scriptWithDifferentChecksum = new Script("fileName", 0L, "xxx",Collections.singleton("PATCH"), "@", "#", "postprocessing");
        assertTrue(script.isScriptContentEqualTo(scriptWithDifferentChecksum, true));
        assertFalse(script.isScriptContentEqualTo(scriptWithDifferentChecksum, false));

        Script scriptWithDifferentChecksumAndModificationDate = new Script("fileName", 1L, "xxx", Collections.singleton("PATCH"), "@", "#", "postprocessing");
        assertFalse(script.isScriptContentEqualTo(scriptWithDifferentChecksumAndModificationDate, true));
        assertFalse(script.isScriptContentEqualTo(scriptWithDifferentChecksumAndModificationDate, false));
    }
    
    private Script createScript(String fileName) {
        Script script = new Script(fileName, 10L, "xxx", Collections.singleton("PATCH"), "@", "#", "postprocessing");
        return script;
    }

}
