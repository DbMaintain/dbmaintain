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
package org.dbmaintain;

import org.dbmaintain.script.Script;
import org.dbmaintain.script.executedscriptinfo.ExecutedScriptInfoSource;
import org.dbmaintain.script.runner.ScriptRunner;
import org.dbmaintain.util.DbMaintainException;
import org.dbmaintain.util.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
class DefaultDbMaintainerScriptErrorTest {

    @Mock
    private ExecutedScriptInfoSource executedScriptInfoSource;

    @Mock
    private ScriptRunner scriptRunner;

    private Script script;


    @BeforeEach
    void initialize() {
        script = TestUtils.createScriptWithContent("01_filename.sql", "content of script");
    }

    @Test
    void errorMessageShouldContainFullScriptContents() {
        DefaultDbMaintainer defaultDbMaintainer = createDefaultDbMaintainer(10000);
        doThrow(new DbMaintainException("error message")).when(scriptRunner).execute(script);

        Throwable e = assertThrows(DbMaintainException.class, () -> defaultDbMaintainer.executeScript(script));

        assertTrue(e.getMessage().contains(
                "Full contents of failed script 01_filename.sql:\n" + "----------------------------------------------------\n"
                        + "content of script\n" + "----------------------------------------------------\n"));
    }

    @Test
    void loggingOfScriptContentsDisabledWhenMaxLengthIsSetTo0() {
        DefaultDbMaintainer defaultDbMaintainer = createDefaultDbMaintainer(0);
        doThrow(new DbMaintainException("error message")).when(scriptRunner).execute(script);

        Throwable e = assertThrows(DbMaintainException.class, () -> defaultDbMaintainer.executeScript(script));
        assertFalse(e.getMessage().contains("Full contents of failed script"));
    }

    @Test
    void largeScriptContentIsTruncated() {
        DefaultDbMaintainer defaultDbMaintainer = createDefaultDbMaintainer(5);
        doThrow(new DbMaintainException("error message")).when(scriptRunner).execute(script);

        Throwable e = assertThrows(DbMaintainException.class, () -> defaultDbMaintainer.executeScript(script));
        assertTrue(e.getMessage().contains(
                "Full contents of failed script 01_filename.sql:\n" + "----------------------------------------------------\n"
                        + "conte... <remainder of script is omitted>\n" + "----------------------------------------------------\n"));

    }

    private DefaultDbMaintainer createDefaultDbMaintainer(long maxNrOfCharsWhenLoggingScriptContent) {
        return new DefaultDbMaintainer(scriptRunner, null, executedScriptInfoSource, false, false, false, false, false,
                false, null, null, null, null, null, null, maxNrOfCharsWhenLoggingScriptContent, null, false, 150);
    }

}
