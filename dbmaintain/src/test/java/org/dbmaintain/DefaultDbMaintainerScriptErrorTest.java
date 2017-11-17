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
import org.junit.Before;
import org.junit.Test;
import org.unitils.UnitilsJUnit4;
import org.unitils.mock.Mock;

import static org.junit.Assert.*;

public class DefaultDbMaintainerScriptErrorTest extends UnitilsJUnit4 {

    protected Mock<ExecutedScriptInfoSource> executedScriptInfoSource;
    protected Mock<ScriptRunner> scriptRunner;

    protected Script script;


    @Before
    public void initialize() {
        script = TestUtils.createScriptWithContent("01_filename.sql", "content of script");
    }


    @Test
    public void errorMessageShouldContainFullScriptContents() throws Exception {
        try {
            DefaultDbMaintainer defaultDbMaintainer = createDefaultDbMaintainer(10000);
            scriptRunner.raises(new DbMaintainException("error message")).execute(script);

            defaultDbMaintainer.executeScript(script);
            fail("Expected DbMaintainException");

        } catch (DbMaintainException e) {
            assertTrue(e.getMessage().contains("Full contents of failed script 01_filename.sql:\n" +
                    "----------------------------------------------------\n" +
                    "content of script\n" +
                    "----------------------------------------------------\n"));
        }
    }


    @Test
    public void loggingOfScriptContentsDisabledWhenMaxLengthIsSetTo0() throws Exception {
        try {
            DefaultDbMaintainer defaultDbMaintainer = createDefaultDbMaintainer(0);
            scriptRunner.raises(new DbMaintainException("error message")).execute(script);

            defaultDbMaintainer.executeScript(script);
            fail("Expected DbMaintainException");

        } catch (DbMaintainException e) {
            assertFalse(e.getMessage().contains("Full contents of failed script"));
        }
    }


    @Test
    public void largeScriptContentIsTruncated() throws Exception {
        try {
            DefaultDbMaintainer defaultDbMaintainer = createDefaultDbMaintainer(5);
            scriptRunner.raises(new DbMaintainException("error message")).execute(script);

            defaultDbMaintainer.executeScript(script);
            fail("Expected DbMaintainException");

        } catch (DbMaintainException e) {
            assertTrue(e.getMessage().contains("Full contents of failed script 01_filename.sql:\n" +
                    "----------------------------------------------------\n" +
                    "conte... <remainder of script is omitted>\n" +
                    "----------------------------------------------------\n"));
        }
    }

    private DefaultDbMaintainer createDefaultDbMaintainer(long maxNrOfCharsWhenLoggingScriptContent) {
        return new DefaultDbMaintainer(scriptRunner.getMock(), null, executedScriptInfoSource.getMock(), false, false, false, false, false,
                false, null, null, null, null, null, null, maxNrOfCharsWhenLoggingScriptContent, null, false, 150);
    }

}
