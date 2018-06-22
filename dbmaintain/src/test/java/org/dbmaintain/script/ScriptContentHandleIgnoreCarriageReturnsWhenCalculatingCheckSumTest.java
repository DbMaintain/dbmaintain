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

import org.junit.jupiter.api.Test;

import static org.dbmaintain.util.TestUtils.createScriptFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 * @author Jessica Austin
 */
class ScriptContentHandleIgnoreCarriageReturnsWhenCalculatingCheckSumTest {


    @Test
    void ignored_stringHandle() {
        Script unixFile = createScriptWithContent("fileName", "script\ncontent", true);
        String unixCheckSum = unixFile.getScriptContentHandle().getCheckSum();

        Script windowsFile = createScriptWithContent("fileName", "script\r\ncontent", true);
        String windowsCheckSum = windowsFile.getScriptContentHandle().getCheckSum();

        assertEquals(windowsCheckSum, unixCheckSum, "CheckSums should be equal for unix and windows");
    }

    @Test
    void ignored_urlHandle() {
        ScriptContentHandle unixScriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("unix-script.sql"), "ISO-8859-1", true);
        ScriptContentHandle windowsScriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("windows-script.sql"), "ISO-8859-1", true);

        String unixCheckSum = unixScriptContentHandle.getCheckSum();
        String windowsCheckSum = windowsScriptContentHandle.getCheckSum();

        assertEquals(windowsCheckSum, unixCheckSum, "CheckSums should be equal for unix and windows");
    }

    @Test
    void notIgnored_stringHandle() {
        Script unixFile = createScriptWithContent("fileName", "script\ncontent", false);
        String unixCheckSum = unixFile.getScriptContentHandle().getCheckSum();

        Script windowsFile = createScriptWithContent("fileName", "script\r\ncontent", false);
        String windowsCheckSum = windowsFile.getScriptContentHandle().getCheckSum();

        assertNotEquals(windowsCheckSum, unixCheckSum, "Scripts should not be converted to UNIX if useUnixCheckSum is false");
    }

    @Test
    void notIgnored_urlHandle() {
        ScriptContentHandle unixScriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("unix-script.sql"), "ISO-8859-1", false);
        ScriptContentHandle windowsScriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("windows-script.sql"), "ISO-8859-1", false);

        String unixCheckSum = unixScriptContentHandle.getCheckSum();
        String windowsCheckSum = windowsScriptContentHandle.getCheckSum();

        assertNotEquals(windowsCheckSum, unixCheckSum, "Scripts should not be converted to UNIX if useUnixCheckSum is false");
    }


    private Script createScriptWithContent(String fileName, String scriptContent, boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
        ScriptFactory scriptFactory = createScriptFactory();
        return scriptFactory.createScriptWithContent(fileName, 0L, new ScriptContentHandle.StringScriptContentHandle(scriptContent, "ISO-8859-1", ignoreCarriageReturnsWhenCalculatingCheckSum));
    }
}
