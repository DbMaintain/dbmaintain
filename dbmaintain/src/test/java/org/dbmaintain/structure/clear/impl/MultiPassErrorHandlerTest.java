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
package org.dbmaintain.structure.clear.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultiPassErrorHandlerTest {

    private MultiPassErrorHandler multiPassErrorHandler = new MultiPassErrorHandler();

    @Test
    public void testAfterFirstPass_WhenErrorsZero_StopExecution() {
        multiPassErrorHandler.addError(new RuntimeException("e1"));
        assertTrue(multiPassErrorHandler.continueExecutionAfterPass());
        assertFalse(multiPassErrorHandler.continueExecutionAfterPass());
    }

    @Test
    public void testAfterFirstPass_WhenErrorsLessThanLastPass_ContinueExecution() {
        multiPassErrorHandler.addError(new RuntimeException("e1"));
        multiPassErrorHandler.addError(new RuntimeException("e2"));
        assertTrue(multiPassErrorHandler.continueExecutionAfterPass());
        multiPassErrorHandler.addError(new RuntimeException("e3"));
        assertTrue(multiPassErrorHandler.continueExecutionAfterPass());
    }


    @Test
    public void testOnFirstPass_WhenNoErrors_StopExecution() {
        assertFalse(multiPassErrorHandler.continueExecutionAfterPass());
    }

    @Test
    public void testOnFirstPass_WhenErrors_ContinueExecution() {
        multiPassErrorHandler.addError(new RuntimeException("e1"));
        assertTrue(multiPassErrorHandler.continueExecutionAfterPass());
    }

    @Test
    public void testAfterFirstPass_WhenErrorsNotLessThanLastPass_ThrowException() {
        multiPassErrorHandler.addError(new RuntimeException("e1"));
        multiPassErrorHandler.continueExecutionAfterPass();
        multiPassErrorHandler.addError(new RuntimeException("e2"));
        assertThrows(RuntimeException.class, () -> multiPassErrorHandler.continueExecutionAfterPass());
    }
}
