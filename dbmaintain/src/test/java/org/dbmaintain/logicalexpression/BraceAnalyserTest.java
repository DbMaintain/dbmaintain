package org.dbmaintain.logicalexpression;

import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class BraceAnalyserTest {

    private ExpressionTokenizer tokenizer = new ExpressionTokenizer();
    private BraceAnalyser braceAnalyser = new BraceAnalyser();

    @Test
    public void analyseBraces() {
        assertBracedSubExpression("a && b", "|| c", "a && b) || c");
        assertBracedSubExpression("a && (b &&c)", "&& d", "a && (b &&c)) && d");
    }

    private void assertBracedSubExpression(String braced, String remainder, String full) {
        assertEquals(new BraceAnalyser.SplitExpression(tokenize(braced), tokenize(remainder)),
                braceAnalyser.splitOffBracedSubExpression(tokenize(full)));
    }

    private List<String> tokenize(String expression) {
        return tokenizer.tokenize(expression);
    }

}
