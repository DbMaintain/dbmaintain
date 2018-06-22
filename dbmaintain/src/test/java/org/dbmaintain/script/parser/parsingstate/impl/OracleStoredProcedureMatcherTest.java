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
package org.dbmaintain.script.parser.parsingstate.impl;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Filip Neven
 */
public class OracleStoredProcedureMatcherTest {

    String[] STORED_PROCEDURE_START_STRINGS = {"CREATE PACKAGE", "CREATE OR REPLACE PACKAGE", "CREATE LIBRARY",
            "CREATE OR REPLACE LIBRARY", "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", "CREATE PROCEDURE",
            "CREATE OR REPLACE PROCEDURE", "CREATE TRIGGER", "CREATE OR REPLACE TRIGGER", "CREATE TYPE",
            "CREATE OR REPLACE TYPE", "DECLARE", "BEGIN"};

    OraclePlSqlBlockMatcher matcher = new OraclePlSqlBlockMatcher();
    Random rnd = new Random();

    @Test
    public void testIsStartOfStoredProcedure() {
        assertIsStartOfStoredProcedure("CREATE PACKAGE", "CREATE OR REPLACE PACKAGE", "CREATE LIBRARY",
                "CREATE OR REPLACE LIBRARY", "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", "CREATE PROCEDURE",
                "CREATE OR REPLACE PROCEDURE", "CREATE TRIGGER", "CREATE OR REPLACE TRIGGER", "CREATE TYPE",
                "CREATE OR REPLACE TYPE", "DECLARE", "BEGIN");
        assertIsNotStartOfStoredProcedure(" CREATE PACKAGE", "CREATE  PACKAGE", "CREATE PACKAGE SOMETHING", "CREATE\nPACKAGE");
    }

    private void assertIsStartOfStoredProcedure(String... testStrings) {
        for (String testString : testStrings) {
            assertTrue(matcher.isStartOfPlSqlBlock(new StringBuilder(testString)));
        }
    }

    private void assertIsNotStartOfStoredProcedure(String... testStrings) {
        for (String testString : testStrings) {
            assertFalse(matcher.isStartOfPlSqlBlock(new StringBuilder(testString)));
        }
    }

    @Test
    public void testPerformance() throws IOException {
        List<String> randomTestCases = createRandomTestStrings(10000);
        //System.out.println("randomTestCases = " + randomTestCases);
        List<String> startOfStoredProcedureTestCases = createStartOfStoredProcedureTestStrings(10000);
        //System.out.println("startOfStoredProcedureTestCases = " + startOfStoredProcedureTestCases);
        List<String> testCases = mix(randomTestCases, startOfStoredProcedureTestCases);
        //System.out.println("testCases = " + testCases);
        int storedProcCount = 0;
        long time = System.nanoTime();
        for (String testCase : testCases) {
            StringReader stringReader = new StringReader(testCase);
            StringBuilder stringBuilder = new StringBuilder();
            int next = stringReader.read();
            while (next != -1) {
                stringBuilder.append((char) next);
                if (isStartOfStoredProcedureOrig(stringBuilder)) {
                    storedProcCount++;
                    continue;
                }
                next = stringReader.read();
            }
        }
        System.out.println("Time = " + ((System.nanoTime() - time) / 1000000) + " milliseconds");
        assertEquals(startOfStoredProcedureTestCases.size(), storedProcCount);
    }

    private List<String> mix(List<String> left, List<String> right) {
        List<String> leftCopy = new ArrayList<>(left);
        List<String> rightCopy = new ArrayList<>(right);
        List<String> results = new ArrayList<>();
        while (!(leftCopy.isEmpty() && rightCopy.isEmpty())) {
            if (leftCopy.isEmpty()) {
                results.add(rightCopy.get(rightCopy.size() - 1));
                rightCopy.remove(rightCopy.size() - 1);
            } else if (rightCopy.isEmpty()) {
                results.add(leftCopy.get(leftCopy.size() - 1));
                leftCopy.remove(leftCopy.size() - 1);
            } else {
                boolean takeLeft = rnd.nextBoolean();
                if (takeLeft) {
                    results.add(leftCopy.get(leftCopy.size() - 1));
                    leftCopy.remove(leftCopy.size() - 1);
                } else {
                    results.add(rightCopy.get(rightCopy.size() - 1));
                    rightCopy.remove(rightCopy.size() - 1);
                }
            }
        }
        return results;
    }

    private List<String> createStartOfStoredProcedureTestStrings(int count) {
        List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            result.add(getRandomStartOfStoredProcedure().toUpperCase() + " " + createRandomTestString(50));
        return result;
    }

    private List<String> createRandomTestStrings(int count) {
        List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            result.add(createRandomTestString(100));
        return result;
    }

    private String createRandomTestString(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++)
            builder.append(getRandomCharacter());
        return builder.toString();
    }

    private char getRandomCharacter() {
        int characterType = rnd.nextInt(8);
        if (characterType < 3)
            return (char) (65 + rnd.nextInt(26));
        else if (characterType < 6)
            return (char) (97 + rnd.nextInt(26));
        else if (characterType < 7)
            return (char) (48 + rnd.nextInt(10));
        else
            return ' ';
    }

    private boolean isStartOfStoredProcedureOrig(StringBuilder statement) {
        return matches("CREATE PACKAGE", statement) || matches("CREATE OR REPLACE PACKAGE", statement) ||
                matches("CREATE LIBRARY", statement) || matches("CREATE OR REPLACE LIBRARY", statement) ||
                matches("CREATE FUNCTION", statement) || matches("CREATE OR REPLACE FUNCTION", statement) ||
                matches("CREATE PROCEDURE", statement) || matches("CREATE OR REPLACE PROCEDURE", statement) ||
                matches("CREATE TRIGGER", statement) || matches("CREATE OR REPLACE TRIGGER", statement) ||
                matches("CREATE TYPE", statement) || matches("CREATE OR REPLACE TYPE", statement) ||
                matches("DECLARE", statement) || matches("BEGIN", statement);
    }


    /**
     * Utility method to check whether the given statement starts with the letters of the given text.
     *
     * @param text      The starting letters, not null
     * @param statement The statement to check
     * @return True if the statement starts with the text
     */
    protected boolean matches(String text, StringBuilder statement) {
        if (text.length() != statement.length()) {
            return false;
        }

        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) != statement.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private String getRandomStartOfStoredProcedure() {
        return STORED_PROCEDURE_START_STRINGS[rnd.nextInt(STORED_PROCEDURE_START_STRINGS.length)];
    }
}
