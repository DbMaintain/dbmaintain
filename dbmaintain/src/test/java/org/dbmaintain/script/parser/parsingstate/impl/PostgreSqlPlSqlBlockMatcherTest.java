package org.dbmaintain.script.parser.parsingstate.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
class PostgreSqlPlSqlBlockMatcherTest {

    private PostgreSqlPlSqlBlockMatcher postgreSqlPlSqlBlockMatcher = new PostgreSqlPlSqlBlockMatcher();

    @Test
    void startOfStoredProcedure() {
        assertIsStartOfStoredProcedure("CREATE FUNCTION");
        assertIsStartOfStoredProcedure("CREATE OR REPLACE FUNCTION");
        assertIsStartOfStoredProcedure("CREATE RULE");
        assertIsStartOfStoredProcedure("CREATE OR REPLACE RULE");
        assertIsStartOfStoredProcedure("BEGIN");
    }

    @Test
    void noStartOfStoredProcedure() {
        assertIsNotStartOfStoredProcedure("DECLARE");
        assertIsNotStartOfStoredProcedure("CREATE PROCEDURE");
        assertIsNotStartOfStoredProcedure("CREATE TRIGGER");
    }

    @Test
    void noStartOfStoredProcedure_spacing() {
        assertIsNotStartOfStoredProcedure(" CREATE FUNCTION");
        assertIsNotStartOfStoredProcedure("CREATE  FUNCTION");
        assertIsNotStartOfStoredProcedure("CREATE FUNCTION SOMETHING");
        assertIsNotStartOfStoredProcedure("CREATE\nFUNCTION");
    }

    private void assertIsStartOfStoredProcedure(String text) {
        assertTrue(postgreSqlPlSqlBlockMatcher.isStartOfPlSqlBlock(new StringBuilder(text)), text);
    }

    private void assertIsNotStartOfStoredProcedure(String text) {
        assertFalse(postgreSqlPlSqlBlockMatcher.isStartOfPlSqlBlock(new StringBuilder(text)), text);
    }
}
