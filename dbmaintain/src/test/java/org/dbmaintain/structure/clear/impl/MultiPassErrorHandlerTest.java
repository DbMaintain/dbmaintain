package org.dbmaintain.structure.clear.impl;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    @Test(expected = RuntimeException.class)
    public void testAfterFirstPass_WhenErrorsNotLessThanLastPass_ThrowException() {
        multiPassErrorHandler.addError(new RuntimeException("e1"));
        multiPassErrorHandler.continueExecutionAfterPass();
        multiPassErrorHandler.addError(new RuntimeException("e2"));
        multiPassErrorHandler.continueExecutionAfterPass();
    }
}
