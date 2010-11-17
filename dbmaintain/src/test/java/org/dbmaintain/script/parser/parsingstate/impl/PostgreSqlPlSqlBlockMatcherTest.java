package org.dbmaintain.script.parser.parsingstate.impl;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class PostgreSqlPlSqlBlockMatcherTest {

    private PostgreSqlPlSqlBlockMatcher postgreSqlPlSqlBlockMatcher = new PostgreSqlPlSqlBlockMatcher();

    @Test
    public void startOfStoredProcedure() {
        assertIsStartOfStoredProcedure("CREATE FUNCTION");
        assertIsStartOfStoredProcedure("CREATE OR REPLACE FUNCTION");
        assertIsStartOfStoredProcedure("CREATE RULE");
        assertIsStartOfStoredProcedure("CREATE OR REPLACE RULE");
        assertIsStartOfStoredProcedure("BEGIN");
    }

    @Test
    public void noStartOfStoredProcedure() {
        assertIsNotStartOfStoredProcedure("DECLARE");
        assertIsNotStartOfStoredProcedure("CREATE PROCEDURE");
        assertIsNotStartOfStoredProcedure("CREATE TRIGGER");
    }

    @Test
    public void noStartOfStoredProcedure_spacing() {
        assertIsNotStartOfStoredProcedure(" CREATE FUNCTION");
        assertIsNotStartOfStoredProcedure("CREATE  FUNCTION");
        assertIsNotStartOfStoredProcedure("CREATE FUNCTION SOMETHING");
        assertIsNotStartOfStoredProcedure("CREATE\nFUNCTION");
    }

    private void assertIsStartOfStoredProcedure(String text) {
        assertTrue(text, postgreSqlPlSqlBlockMatcher.isStartOfPlSqlBlock(new StringBuilder(text)));
    }

    private void assertIsNotStartOfStoredProcedure(String text) {
        assertFalse(text, postgreSqlPlSqlBlockMatcher.isStartOfPlSqlBlock(new StringBuilder(text)));
    }
}
