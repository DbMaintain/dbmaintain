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

import org.dbmaintain.script.qualifier.Qualifier;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 * @author Jessica Austin
 */
public class ScriptContentHandleIgnoreCarriageReturnsWhenCalculatingCheckSumTest {


    @Test
    public void ignored_stringHandle() {
        Script unixFile = createScriptWithContent("fileName", "script\ncontent", true);
        String unixCheckSum = unixFile.getScriptContentHandle().getCheckSum();

        Script windowsFile = createScriptWithContent("fileName", "script\r\ncontent", true);
        String windowsCheckSum = windowsFile.getScriptContentHandle().getCheckSum();

        assertEquals("CheckSums should be equal for unix and windows", windowsCheckSum, unixCheckSum);
    }

    @Test
    public void ignored_urlHandle() {
        ScriptContentHandle unixScriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("unix-script.sql"), "ISO-8859-1", true);
        ScriptContentHandle windowsScriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("windows-script.sql"), "ISO-8859-1", true);

        String unixCheckSum = unixScriptContentHandle.getCheckSum();
        String windowsCheckSum = windowsScriptContentHandle.getCheckSum();

        assertEquals("CheckSums should be equal for unix and windows", windowsCheckSum, unixCheckSum);
    }

    @Test
    public void notIgnored_stringHandle() {
        Script unixFile = createScriptWithContent("fileName", "script\ncontent", false);
        String unixCheckSum = unixFile.getScriptContentHandle().getCheckSum();

        Script windowsFile = createScriptWithContent("fileName", "script\r\ncontent", false);
        String windowsCheckSum = windowsFile.getScriptContentHandle().getCheckSum();

        assertFalse("Scripts should not be converted to UNIX if useUnixCheckSum is false", windowsCheckSum.equals(unixCheckSum));
    }

    @Test
    public void notIgnored_urlHandle() {
        ScriptContentHandle unixScriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("unix-script.sql"), "ISO-8859-1", false);
        ScriptContentHandle windowsScriptContentHandle = new ScriptContentHandle.UrlScriptContentHandle(getClass().getResource("windows-script.sql"), "ISO-8859-1", false);

        String unixCheckSum = unixScriptContentHandle.getCheckSum();
        String windowsCheckSum = windowsScriptContentHandle.getCheckSum();

        assertFalse("Scripts should not be converted to UNIX if useUnixCheckSum is false", windowsCheckSum.equals(unixCheckSum));
    }


    private Script createScriptWithContent(String fileName, String scriptContent, boolean ignoreCarriageReturnsWhenCalculatingCheckSum) {
        return new Script(fileName, 0L, new ScriptContentHandle.StringScriptContentHandle(scriptContent, "ISO-8859-1", ignoreCarriageReturnsWhenCalculatingCheckSum),
                "@", "#", new HashSet<Qualifier>(), new HashSet<Qualifier>(), "postprocessing", null);
    }
}
