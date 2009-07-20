package org.dbmaintain.scriptparser.parsingstate.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * @author Filip Neven
 */
public class OracleStoredProcedureMatcherTest {

    OracleStoredProcedureMatcher matcher = new OracleStoredProcedureMatcher();

    @Test
    public void testIsStartOfStoredProcedure() {
        assertIsStartOfStoredProcedure("CREATE PACKAGE", "create or replace package", "create library",
                "create or replace library", "create function", "create or replace function", "create procedure",
                "create or replace procedure", "create trigger", "create or replace trigger", "create type",
                "create or replace type", "declare", "begin");
        assertIsNotStartOfStoredProcedure(" create package", "create  package", "create package something", "create\npackage");
    }

    private void assertIsStartOfStoredProcedure(String... testStrings) {
        for (String testString : testStrings) {
            assertTrue(matcher.isStartOfStoredProcedure(testString));
        }
    }

    private void assertIsNotStartOfStoredProcedure(String... testStrings) {
        for (String testString : testStrings) {
            assertFalse(matcher.isStartOfStoredProcedure(testString));
        }
    }
}
